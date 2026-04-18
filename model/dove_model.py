import torch
import torch.nn as nn
import torch.nn.functional as F



# ============================================================
#  RMSNorm  (modern alternative to LayerNorm)
# ============================================================
class RMSNorm(nn.Module):
    def __init__(self, dim, eps=1e-8):
        super().__init__()
        self.scale = nn.Parameter(torch.ones(dim))
        self.eps = eps

    def forward(self, x):
        norm_x = x * torch.rsqrt(x.pow(2).mean(-1, keepdim=True) + self.eps)
        return self.scale * norm_x


# ============================================================
#  SwiGLU Feedforward Block
# ============================================================
class SwiGLU(nn.Module):
    def __init__(self, dim, hidden_dim, dropout=0.1):
        super().__init__()
        self.fc1 = nn.Linear(dim, hidden_dim * 2)
        self.fc2 = nn.Linear(hidden_dim, dim)
        self.dropout = nn.Dropout(dropout)

    def forward(self, x):
        x_proj, gate = self.fc1(x).chunk(2, dim=-1)
        return self.fc2(self.dropout(F.silu(gate) * x_proj))


# ============================================================
#  ALiBi Positional Bias
# ============================================================
def build_alibi_bias(n_heads, seq_len, device):
    slopes = torch.pow(2, torch.linspace(-3, 3, n_heads, device=device))
    pos = torch.arange(seq_len, device=device)
    bias = pos[None, :] - pos[:, None]
    bias = bias.abs().float()
    return -bias[None, None, :, :] * slopes[:, None, None]


# ============================================================
#  Modern Transformer Layer
# ============================================================
class ModernTransformerLayer(nn.Module):
    def __init__(self, d_model, nhead, dropout=0.1, ffn_factor=4):
        super().__init__()
        self.nhead = nhead
        self.head_dim = d_model // nhead
        self.qkv_proj = nn.Linear(d_model, d_model * 3)
        self.o_proj = nn.Linear(d_model, d_model)
        self.norm1 = RMSNorm(d_model)
        self.norm2 = RMSNorm(d_model)
        self.ffn = SwiGLU(d_model, int(d_model * ffn_factor), dropout)
        self.dropout = nn.Dropout(dropout)

    def forward(self, x, attn_mask=None, alibi_bias=None):
        B, L, D = x.shape
        x_norm = self.norm1(x)

        # Project Q, K, V
        qkv = self.qkv_proj(x_norm)
        q, k, v = qkv.chunk(3, dim=-1)
        q = q.view(B, L, self.nhead, self.head_dim).transpose(1, 2)
        k = k.view(B, L, self.nhead, self.head_dim).transpose(1, 2)
        v = v.view(B, L, self.nhead, self.head_dim).transpose(1, 2)

        # Compute attention manually (to support ALiBi)
        attn_scores = torch.matmul(q, k.transpose(-2, -1)) / (self.head_dim ** 0.5)
        if alibi_bias is not None:
            # ✅ Add ALiBi bias here (same shape as attn_scores)
            attn_scores = attn_scores + alibi_bias[:, :, :L, :L]

        if attn_mask is not None:
            attn_scores = attn_scores.masked_fill(attn_mask, float('-inf'))

        attn_weights = torch.softmax(attn_scores, dim=-1)
        attn_weights = self.dropout(attn_weights)

        attn_output = torch.matmul(attn_weights, v)
        attn_output = attn_output.transpose(1, 2).reshape(B, L, D)

        # Output projection + FFN with residuals
        x = x + self.dropout(self.o_proj(attn_output))
        x = x + self.dropout(self.ffn(self.norm2(x)))
        return x



# ============================================================
#  Attention Pooling
# ============================================================
class AttentionPooling(nn.Module):
    def __init__(self, d_model):
        super().__init__()
        self.attn = nn.Linear(d_model, 1)

    def forward(self, x, seq_mask=None):
        scores = self.attn(x).squeeze(-1)
        if seq_mask is not None:
            scores = scores.masked_fill(seq_mask, float("-inf"))
        weights = torch.softmax(scores, dim=1).unsqueeze(-1)
        return (x * weights).sum(dim=1)


# ============================================================
#  Modernized DovePeakPredictor
# ============================================================
class DovePeakPredictor(nn.Module):
    def __init__(self, num_doves=500, seq_len=150, num_features=10,
                 dove_embed_dim=128, d_model=128, nhead=4, num_layers=4,
                 dropout=0.2):
        super().__init__()
        self.seq_len = seq_len

        # Dove embedding
        self.dove_embed = nn.Embedding(num_doves + 1, dove_embed_dim)

        # Input projection: features + mask + embedding
        self.input_proj = nn.Linear(num_features * 2 + dove_embed_dim, d_model)
        self.input_norm = RMSNorm(d_model)

        # ALiBi bias (relative positional)
        self.register_buffer("alibi_bias", build_alibi_bias(nhead, seq_len, "cpu"))

        # Stack of modern Transformer layers
        self.layers = nn.ModuleList([
            ModernTransformerLayer(d_model, nhead, dropout)
            for _ in range(num_layers)
        ])

        # Attention pooling
        self.attn_pool = AttentionPooling(d_model)

        # Classification head
        self.classifier = nn.Sequential(
            nn.Dropout(dropout),
            nn.Linear(d_model, 64),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(64, 7), # 7 classes
        )

    def forward(self, x, dove_ids, seq_mask=None):
        feature_mask = (~torch.isnan(x)).float()
        x = torch.nan_to_num(x, nan=0.0)
        x = torch.cat([x, feature_mask], dim=-1)

        # Dove embeddings
        dove_emb = self.dove_embed(dove_ids).unsqueeze(1).expand(-1, self.seq_len, -1)
        x = torch.cat([x, dove_emb], dim=-1)

        x = self.input_proj(x)
        x = self.input_norm(x)

        alibi_bias = self.alibi_bias.to(x.device)

        for layer in self.layers:
            x = layer(x, attn_mask=seq_mask, alibi_bias=alibi_bias)

        x = self.attn_pool(x, seq_mask)
        return self.classifier(x)


# ============================================================
#  Example Usage
# ============================================================
if __name__ == "__main__":
    model = DovePeakPredictor()
    batch_size = 8
    x = torch.randn(batch_size, 150, 10)
    dove_ids = torch.randint(0, 500, (batch_size,))
    seq_mask = torch.zeros(batch_size, 150, dtype=torch.bool)
    out = model(x, dove_ids, seq_mask)
    print(out.shape)   # (batch_size, 7)

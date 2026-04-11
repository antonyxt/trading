# ✅ 1. Modern C++ (C++11–20/23) – MUST KNOW
**Why?** Companies want engineers who write efficient, clean, modern code.

## 🔹 Core Features
- `auto`, `decltype`, range-based for loops  
- `nullptr`, scoped enums (`enum class`)  
- `constexpr` and `consteval`  
- Lambda expressions (generic lambdas, captures, mutable)  
- Structured bindings (`auto [x, y] = ...`)  
- `if constexpr`, concepts, `requires`  

## 🔹 Smart Pointers
- `std::unique_ptr`, `std::shared_ptr`, `std::weak_ptr`  
- Ownership semantics, custom deleters, cyclic references  

## 🔹 Move Semantics
- Rvalue references (`T&&`)  
- Move constructors, move assignment  
- `std::move`, `std::forward`  
- Perfect forwarding, universal references  

---

# ✅ 2. Object-Oriented Design
**Why?** OOP is the backbone of many large-scale systems.

- Virtual functions and vtables  
- Inheritance, polymorphism, dynamic casting  
- Object slicing  
- Rule of Three / Five / Zero  
- CRTP (Curiously Recurring Template Pattern)  

---

# ✅ 3. Templates and Metaprogramming
**Why?** Templates power generic programming and STL.

- Function/class templates  
- Template specialization & partial specialization  
- SFINAE (`std::enable_if`, overload resolution)  
- Concepts (C++20)  
- Variadic templates (`template<typename... Args>`)  
- Fold expressions (C++17)  

---

# ✅ 4. Concurrency and Multithreading
**Why?** Performance-critical code is often parallel.

- `std::thread`, `std::mutex`, `std::lock_guard`, `std::unique_lock`  
- `std::condition_variable`, `std::atomic`  
- `std::promise`, `std::future`, `std::packaged_task`  
- Thread safety, data races, deadlocks  
- Thread pools, task scheduling  

---

# ✅ 5. STL (Standard Template Library)
**Why?** Knowing STL saves time and improves performance.

- Containers: `vector`, `list`, `deque`, `set`, `map`, `unordered_map`, `queue`, etc.  
- Iterators and ranges  
- Algorithms: `sort`, `find_if`, `transform`, `reduce`  
- Custom comparators and hashers  
- `std::function`, `std::bind`, `std::invoke`  
- Allocators (basic knowledge is often enough)  

---

# ✅ 6. Memory Management and Optimization
**Why?** C++ gives full control over memory — this is often tested.

- Stack vs heap, memory layout, alignment  
- `new`, `delete`, `malloc`, `free`, placement new  
- RAII  
- Cache locality, data-oriented design  
- Small object optimization (SOO), copy elision  

---

# ✅ 7. Compile-time Programming
**Why?** High-performance systems use compile-time logic to avoid runtime cost.

- `constexpr` functions  
- Type traits and `<type_traits>`  
- Static assertions  
- Template metaprogramming basics  
- Tag dispatching, type deduction  

---

# ✅ 8. Design Patterns (C++ idioms)
**Why?** Design decisions matter as much as implementation.

- Pimpl idiom  
- RAII  
- Factory, Singleton, Observer, Strategy  
- Non-copyable base class (delete copy/move constructors)  
- CRTP, policy-based design  

---

# ✅ 9. Tooling and Compilation
**Why?** You’ll often be expected to work close to the system.

- Build systems: Make, CMake, Ninja  
- Preprocessor macros, `#include` guard vs `#pragma once`  
- Static and dynamic libraries  
- Linking process, symbol visibility  
- Debugging with `gdb` or Visual Studio  
- Compiler flags (e.g., `-O2`, `-std=c++20`, warnings)  

---

# ✅ 10. C++20/23 New Features (Bonus)
**Why?** Shows you're staying current.

- `std::span`  
- `std::ranges`, `std::views`  
- Coroutines (`co_await`, `co_yield`)  
- Modules  
- `std::format`, `std::chrono` improvements  
- `std::expected` (C++23)  

---

# ✅ 11. Testing and Best Practices

- Unit testing frameworks (Google Test, Catch2)  
- Assertions, test coverage  
- Writing exception-safe code (strong/basic guarantee)  
- Coding guidelines (e.g., C++ Core Guidelines)  
- Profiling tools (valgrind, perf, Instruments, etc.)  

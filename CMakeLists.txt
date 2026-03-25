cmake_minimum_required(VERSION 3.16)
project(kworker_core VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ── Core library (static) ──
add_library(kworker_lib STATIC
    cpp/src/scheduler.cpp
    cpp/src/dag.cpp
)
target_include_directories(kworker_lib PUBLIC cpp/include)

# ── C++ tests ──
option(BUILD_TESTS "Build C++ tests" ON)
if(BUILD_TESTS)
    enable_testing()

    add_executable(test_scheduler cpp/tests/test_scheduler.cpp)
    target_link_libraries(test_scheduler PRIVATE kworker_lib)
    add_test(NAME test_scheduler COMMAND test_scheduler)
endif()

# ── pybind11 module ──
option(BUILD_PYTHON "Build Python bindings" ON)
if(BUILD_PYTHON)
    find_package(pybind11 QUIET)
    if(NOT pybind11_FOUND)
        # Fallback: use pybind11 from pip
        execute_process(
            COMMAND python3 -m pybind11 --cmakedir
            OUTPUT_VARIABLE pybind11_DIR
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        find_package(pybind11 REQUIRED PATHS ${pybind11_DIR})
    endif()

    pybind11_add_module(_kworker_core cpp/src/bindings.cpp)
    target_link_libraries(_kworker_core PRIVATE kworker_lib)

    # Install the .so next to the Python package
    install(TARGETS _kworker_core DESTINATION python/kworker)
endif()

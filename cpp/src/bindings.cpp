#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "kworker/scheduler.hpp"
#include "kworker/task.hpp"
#include "kworker/dag.hpp"

namespace py = pybind11;

PYBIND11_MODULE(_kworker_core, m) {
    m.doc() = "KWorker C++ scheduling engine";

    // ── TaskState enum ──
    py::enum_<kworker::TaskState>(m, "TaskState")
        .value("PENDING", kworker::TaskState::PENDING)
        .value("SCHEDULED", kworker::TaskState::SCHEDULED)
        .value("RUNNING", kworker::TaskState::RUNNING)
        .value("COMPLETED", kworker::TaskState::COMPLETED)
        .value("FAILED", kworker::TaskState::FAILED)
        .value("RETRYING", kworker::TaskState::RETRYING)
        .value("DEAD", kworker::TaskState::DEAD)
        .export_values();

    // ── RetryPolicy ──
    py::class_<kworker::RetryPolicy>(m, "RetryPolicy")
        .def(py::init<>())
        .def_readwrite("max_attempts", &kworker::RetryPolicy::max_attempts)
        .def_readwrite("backoff_base", &kworker::RetryPolicy::backoff_base)
        .def_readwrite("backoff_max", &kworker::RetryPolicy::backoff_max)
        .def("delay_for_attempt", &kworker::RetryPolicy::delay_for_attempt);

    // ── Task ──
    py::class_<kworker::Task>(m, "Task")
        .def(py::init<>())
        .def_readwrite("id", &kworker::Task::id)
        .def_readwrite("queue", &kworker::Task::queue)
        .def_readwrite("handler_name", &kworker::Task::handler_name)
        .def_readwrite("payload_json", &kworker::Task::payload_json)
        .def_readwrite("priority", &kworker::Task::priority)
        .def_readwrite("state", &kworker::Task::state)
        .def_readwrite("created_at_ms", &kworker::Task::created_at_ms)
        .def_readwrite("deadline_ms", &kworker::Task::deadline_ms)
        .def_readwrite("scheduled_at_ms", &kworker::Task::scheduled_at_ms)
        .def_readwrite("started_at_ms", &kworker::Task::started_at_ms)
        .def_readwrite("completed_at_ms", &kworker::Task::completed_at_ms)
        .def_readwrite("attempt", &kworker::Task::attempt)
        .def_readwrite("retry_policy", &kworker::Task::retry_policy)
        .def_readwrite("depends_on", &kworker::Task::depends_on)
        .def("compute_score", &kworker::Task::compute_score,
             py::arg("now_ms"),
             "Compute scheduling score (lower = more urgent)");

    // ── DAGResolver ──
    py::class_<kworker::DAGResolver>(m, "DAGResolver")
        .def(py::init<>())
        .def("add_task", &kworker::DAGResolver::add_task)
        .def("mark_completed", &kworker::DAGResolver::mark_completed)
        .def("mark_failed", &kworker::DAGResolver::mark_failed)
        .def("is_ready", &kworker::DAGResolver::is_ready)
        .def("get_ready_tasks", &kworker::DAGResolver::get_ready_tasks)
        .def("get_pending_deps", &kworker::DAGResolver::get_pending_deps)
        .def("would_create_cycle", &kworker::DAGResolver::would_create_cycle)
        .def("remove_task", &kworker::DAGResolver::remove_task)
        .def("clear", &kworker::DAGResolver::clear)
        .def("__len__", &kworker::DAGResolver::size);

    // ── Scheduler ──
    py::class_<kworker::Scheduler>(m, "Scheduler")
        .def(py::init<size_t>(), py::arg("max_capacity") = 10000)
        .def("add_task", &kworker::Scheduler::add_task)
        .def("add_tasks", &kworker::Scheduler::add_tasks)
        .def("next_task", &kworker::Scheduler::next_task,
             "Get next ready task, or None if empty")
        .def("peek", &kworker::Scheduler::peek,
             py::arg("n") = 10)
        .def("complete_task", &kworker::Scheduler::complete_task)
        .def("fail_task", &kworker::Scheduler::fail_task,
             "Mark task failed; returns list of now-unrunnable task IDs")
        .def("remove_task", &kworker::Scheduler::remove_task)
        .def("calculate_retry_delay", &kworker::Scheduler::calculate_retry_delay)
        .def("score_tasks", &kworker::Scheduler::score_tasks)
        .def("pending_count", &kworker::Scheduler::pending_count)
        .def("ready_count", &kworker::Scheduler::ready_count)
        .def("clear", &kworker::Scheduler::clear);
}

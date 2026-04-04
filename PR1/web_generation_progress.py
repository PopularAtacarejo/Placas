from __future__ import annotations

import time
from datetime import datetime
from threading import Lock
from typing import Any, Dict, List


def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class GenerationProgressTracker:
    def __init__(self) -> None:
        self._lock = Lock()
        self._phase = "idle"
        self._phase_label = "Aguardando"
        self._phase_detail = ""
        self._started_perf = 0.0
        self._started_at = ""
        self._finished_at = ""
        self._selected_items = 0
        self._predicted_copies = 0
        self._total_jobs = 0
        self._completed_jobs = 0
        self._failed_jobs = 0
        self._current_job_index = 0
        self._current_plate_name = ""
        self._current_plate_format = ""
        self._last_output = ""
        self._last_error = ""
        self._failed_items: List[Dict[str, Any]] = []

    def _elapsed_seconds_unlocked(self) -> float:
        if self._phase in {"running", "config_submitted"} and self._started_perf > 0:
            return max(0.0, time.perf_counter() - self._started_perf)
        if self._started_perf <= 0:
            return 0.0
        return max(0.0, time.perf_counter() - self._started_perf)

    def mark_config_submitted(self, selected_items: int, predicted_copies: int) -> None:
        with self._lock:
            self._phase = "config_submitted"
            self._phase_label = "Configuracao recebida"
            self._phase_detail = "Preparando a geracao das placas."
            self._selected_items = max(0, int(selected_items))
            self._predicted_copies = max(0, int(predicted_copies))
            self._started_perf = time.perf_counter()
            self._started_at = _now_iso()
            self._finished_at = ""
            self._completed_jobs = 0
            self._failed_jobs = 0
            self._current_job_index = 0
            self._current_plate_name = ""
            self._current_plate_format = ""
            self._last_output = ""
            self._last_error = ""
            self._failed_items = []

    def start(self, total_jobs: int) -> None:
        with self._lock:
            if self._started_perf <= 0:
                self._started_perf = time.perf_counter()
                self._started_at = _now_iso()
            self._phase = "running"
            self._phase_label = "Gerando placas"
            self._phase_detail = "Criando os arquivos das placas."
            self._total_jobs = max(0, int(total_jobs))

    def set_current(self, job_index: int, total_jobs: int, plate_name: str, plate_format: str) -> None:
        with self._lock:
            self._phase = "running"
            self._phase_label = "Gerando placas"
            self._phase_detail = f"Gerando {str(plate_name or '').strip()} ({str(plate_format or '').strip()})."
            self._total_jobs = max(self._total_jobs, int(total_jobs))
            self._current_job_index = max(0, int(job_index))
            self._current_plate_name = str(plate_name or "")
            self._current_plate_format = str(plate_format or "")

    def set_phase(self, phase: str, label: str = "", detail: str = "") -> None:
        with self._lock:
            self._phase = str(phase or self._phase or "idle")
            if label:
                self._phase_label = str(label)
            if detail or detail == "":
                self._phase_detail = str(detail or "")

    def mark_success(self, output_name: str = "") -> None:
        with self._lock:
            self._completed_jobs += 1
            self._last_output = str(output_name or "")
            self._last_error = ""

    def mark_failure(self, plate_name: str, error_message: str, job_index: int = 0) -> None:
        with self._lock:
            self._failed_jobs += 1
            self._last_error = str(error_message or "")
            item = {
                "job_index": max(0, int(job_index)),
                "plate_name": str(plate_name or ""),
                "error": str(error_message or ""),
                "time": _now_iso(),
            }
            self._failed_items.append(item)
            if len(self._failed_items) > 20:
                self._failed_items = self._failed_items[-20:]

    def finish(self, phase: str = "finished") -> None:
        with self._lock:
            self._phase = str(phase or "finished")
            if self._phase == "finished":
                self._phase_label = "Concluido"
                self._phase_detail = "Todas as etapas foram finalizadas."
            elif self._phase == "finished_with_errors":
                self._phase_label = "Concluido com erros"
                self._phase_detail = "Algumas placas ou impressoes falharam."
            elif self._phase == "cancelled":
                self._phase_label = "Cancelado"
                self._phase_detail = "O processo foi cancelado."
            elif self._phase == "stopped":
                self._phase_label = "Interrompido"
                self._phase_detail = "O processo foi interrompido."
            self._finished_at = _now_iso()
            self._current_plate_name = ""
            self._current_plate_format = ""

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            elapsed_seconds = round(self._elapsed_seconds_unlocked(), 2)
            processed_jobs = max(0, int(self._completed_jobs + self._failed_jobs))
            total_jobs = max(0, int(self._total_jobs))
            progress_percent = 0.0
            estimated_remaining_seconds = 0.0
            if total_jobs > 0:
                progress_percent = min(100.0, max(0.0, (processed_jobs / total_jobs) * 100.0))
                if processed_jobs > 0 and processed_jobs < total_jobs and elapsed_seconds > 0:
                    avg_seconds_per_job = elapsed_seconds / processed_jobs
                    estimated_remaining_seconds = max(0.0, (total_jobs - processed_jobs) * avg_seconds_per_job)
            return {
                "phase": self._phase,
                "phase_label": self._phase_label,
                "phase_detail": self._phase_detail,
                "started_at": self._started_at,
                "finished_at": self._finished_at,
                "elapsed_seconds": elapsed_seconds,
                "selected_items": self._selected_items,
                "predicted_copies": self._predicted_copies,
                "total_jobs": total_jobs,
                "completed_jobs": self._completed_jobs,
                "failed_jobs": self._failed_jobs,
                "processed_jobs": processed_jobs,
                "progress_percent": round(progress_percent, 2),
                "estimated_remaining_seconds": round(estimated_remaining_seconds, 2),
                "current_job_index": self._current_job_index,
                "current_plate_name": self._current_plate_name,
                "current_plate_format": self._current_plate_format,
                "last_output": self._last_output,
                "last_error": self._last_error,
                "failed_items": list(self._failed_items),
            }

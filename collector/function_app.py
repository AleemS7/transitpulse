import azure.functions as func

from transitpulse_collector.settings import Settings
from transitpulse_collector.wiring import build_collector

app = func.FunctionApp()


@app.timer_trigger(schedule="0 */2 * * * *", arg_name="timer")
def realtime(timer: func.TimerRequest) -> None:
    build_collector(Settings.from_env()).run_realtime()


@app.timer_trigger(schedule="0 0 9 * * *", arg_name="timer")
def static_schedule(timer: func.TimerRequest) -> None:
    build_collector(Settings.from_env()).run_static()

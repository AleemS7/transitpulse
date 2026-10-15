import azure.functions as func

app = func.FunctionApp()


# Collector imports stay inside the functions so the host can always register the timers;
# an import problem then surfaces as a run failure with a traceback instead of an empty app.


@app.timer_trigger(schedule="0 */2 * * * *", arg_name="timer")
def realtime(timer: func.TimerRequest) -> None:
    from transitpulse_collector.settings import Settings
    from transitpulse_collector.wiring import build_collector

    build_collector(Settings.from_env()).run_realtime()


@app.timer_trigger(schedule="0 0 9 * * *", arg_name="timer")
def static_schedule(timer: func.TimerRequest) -> None:
    from transitpulse_collector.settings import Settings
    from transitpulse_collector.wiring import build_collector

    build_collector(Settings.from_env()).run_static()

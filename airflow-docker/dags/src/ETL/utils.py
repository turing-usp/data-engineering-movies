from math import ceil
from dateutil.relativedelta import relativedelta
from datetime import datetime

def gera_intervalos(inicio, fim, qt_intervalos):
    tamanho_intervalo = ceil((fim - inicio) / qt_intervalos)
    intervalos = [(i, i + tamanho_intervalo) for i in range(inicio, fim, tamanho_intervalo)]

    if intervalos[-1][1] > fim:
        x = intervalos.pop()
        intervalos.append((x[0], fim))
    return intervalos

def generate_date_intervals(initial_date: str, end_date: str, delta_time: int):
    """
    Generates a list of date intervals between `initial_date` and `end_date` with `delta_time` increments.

    initial_date: str
        'YYYY-mm-dd'
    
    end_date: str
        'YYYY-mm-dd'

    delta_time: int
        in months
    """

    intervals = []

    date_format =  '%Y-%m-%d'
    initial_datetime = datetime.strptime(initial_date, date_format)
    end_datetime = datetime.strptime(end_date, date_format)
    
    current_date = initial_datetime
    while current_date < end_datetime:
        intervals.append((current_date.strftime(date_format), (current_date + relativedelta(months=delta_time)).strftime(date_format)))
        current_date += relativedelta(months=delta_time)
    return intervals
from typing import Tuple, List

def quicksort(I: List[int]):
    Ip = I.copy()
    return quicksortdo(Ip)

def quicksortdo(Ip: List[int], start: int = None, stop: int = None):
    if start == None and stop == None:
        start = 0
        stop = len(Ip) - 1
    i = start
    j = stop
    pivot: int = Ip[int((start + stop)/2)] if len(Ip) % 2 == 1 else Ip[int(((start + stop + 1)/2))]
    while i < j :
        while Ip[i] < pivot:
            i += 1
        while Ip[j] > pivot:
            j = j - 1
        if i <= j :
            Ip[i], Ip[j] = Ip[j], Ip[i] 
            i = i + 1
            j = j - 1
    if start < j:
        quicksortdo(Ip, start, j)
    if i < stop:
        quicksortdo(Ip, i, stop)
    return Ip

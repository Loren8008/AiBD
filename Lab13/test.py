import pytest
from app import quicksort

In1 = [1, 2, 3] #does it break sorted data
Out1 = [1, 2, 3]

In2 = [1, 2, 2, 2, 3, 3, 4, 4, 4, 5, 6, 7, 8, 8, 8] #does it work with equal elements
Out2 = [1, 2, 2, 2, 3, 3, 4, 4, 4, 5, 6, 7, 8, 8, 8]

In3 = [5, 1, 0, -2, 4, 3, 2, 8, 10.5] #does it sort (with negative numbers)
Out3 = [-2, 0, 1, 2, 3, 4, 5, 8, 10.5]

In4 = [-1, -2, 0, 2, 1, 4, 3, 4, 5, -1, 3, 0, 4, 8, -1, 2, 4, 4] #general data
Out4 = [-2, -1, -1, -1, 0, 0, 1, 2, 2, 3, 3, 4, 4, 4, 4, 4, 5, 8]

test_data = [(In1,Out1),(In2,Out2),(In3,Out3),(In4,Out4)]

@pytest.mark.parametrize('input_, expected_output', test_data)
def test_bubblesort(input_,expected_output):
    assert (expected_output == quicksort(input_))
    

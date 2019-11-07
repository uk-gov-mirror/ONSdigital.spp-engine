import mock
from random import randint

vals = [i + 1 for i in range(randint(1, 100))]

def return_val():
    return vals.pop(0)


class TestClass():
    val = None

    def __init__(self):
        self.val = "This sucks"


@mock.patch('__main__.TestClass')
def test_testclass(thing):
    thing.return_value.val = "IT STILL SUCKS"
    print(TestClass().val)
    p = mock.PropertyMock(side_effect=vals)
    type(TestClass()).val = p
    for val in vals:
        print(TestClass().val)
        #print(TestClass().val)


test_testclass()
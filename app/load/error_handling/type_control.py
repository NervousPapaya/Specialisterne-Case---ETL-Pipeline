
def test_parameter(parameter, parameter_type):
    if parameter_type is int:
        if not isinstance(parameter ,int):
            raise TypeError(f"The {parameter} must be an integer")
        elif parameter <= 0:
            raise ValueError(f"The {parameter} must be an integer greater than 0")
    if parameter_type is str:
        if not isinstance(parameter ,str):
            raise TypeError(f"The {parameter} must be a string")

def test_parameters(parameters: list, types: list):
    for parameter, parameter_type in zip(parameters, types):
        if parameter is None:
            pass
        else:
            test_parameter(parameter, parameter_type)

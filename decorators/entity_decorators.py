import functools

def not_implemented_method(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        method_name = method.__name__
        class_name = self.__class__.__name__

        if wrapper._not_implemented:
            raise NotImplementedError(f"Entity@{class_name}.{method_name}() Not Implemented!")
        
        return method(self, *args, **kwargs)
    
    wrapper._not_implemented = True
    return wrapper


def not_implemented_class_method(method):
    def wrapper(cls, *args, **kwargs):
        method_name = method.__name__
        class_name = cls.__name__

        if wrapper._not_implemented:
            raise NotImplementedError(f"Entity@{class_name}.{method_name}() Not Implemented!")
        
        return method(cls, *args, **kwargs)
    
    wrapper._not_implemented = True
    functools.update_wrapper(wrapper, method, assigned=functools.WRAPPER_ASSIGNMENTS)
    return classmethod(wrapper)

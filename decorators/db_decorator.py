
import functools

def recursion_limiter(max_depth: int = 5):
    """
    A decorator to limit the recursion depth of a function.
    
    Parameters:
    max_depth (int): The maximum allowed depth of recursion.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not hasattr(wrapper, 'depth'):
                wrapper.depth = 0
            else:
                wrapper.depth += 1
            
            if wrapper.depth > max_depth:
                class_name = None
                method_name = func.__qualname__
                if '.' in method_name:
                    class_name, method_name = method_name.rsplit('.', 1)
                class_name_str = f"@{class_name}." if class_name else ""
                error_message = f"Recursion depth limit exceed [{max_depth}]: {class_name_str}{method_name}()"
                raise RecursionError(error_message)
            
            try:
                result = func(*args, **kwargs)
            finally:
                wrapper.depth -= 1     
            return result
        return wrapper
    return decorator
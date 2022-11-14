
import importlib

def create_instance(full_name, kwargs):

    i = full_name.rindex(".")
    module_name = full_name[:i]
    class_name = full_name[i + 1:]
    module = importlib.import_module(module_name)
    class_ = getattr(module, class_name)
    instance = class_(**kwargs)
    return instance

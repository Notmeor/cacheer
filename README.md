# cacheer
On-disk caching


## Installation
```
pip install git+https://github.com/Notmeor/cacheer.git
```

## Usage
```python
from cacheer.manager import cache_manager

@cache_manager.cache()
def some_function(*args, **kw):
    pass
```

Check whether we're in caching mode
```python
cache_manager.is_using_cache()
```

Disable caching
```python
cache_manager.disable()
```

Enable cacheing when it's previously disabled
```python
cache_manager.enable()
```

Disable caching with context manager
```python
with cache_manager.cache.no_cache():
    some_function()
```

Run a function while declaring its existing cache outdated (so its cache would be recreated/updated after this call)
```python
with cache_manager.cache.outdate():
    some_function()
```

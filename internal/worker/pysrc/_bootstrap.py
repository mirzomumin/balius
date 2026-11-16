# Installs the importer serving the Python modules compiled into the tulpar
# binary. Executed once, right after the interpreter starts, with SOURCES
# bound to a `{module name: (source, is_package)}` mapping.
import sys
from importlib.abc import Loader, MetaPathFinder
from importlib.machinery import ModuleSpec


class EmbeddedImporter(MetaPathFinder, Loader):
    def __init__(self, sources):
        self._sources = sources

    @staticmethod
    def _origin(fullname, is_package):
        path = fullname.replace('.', '/')
        return f'<tulpar>/{path}/__init__.py' if is_package else f'<tulpar>/{path}.py'

    def find_spec(self, fullname, path=None, target=None):
        entry = self._sources.get(fullname)
        if entry is None:
            return None
        return ModuleSpec(fullname, self, origin=self._origin(fullname, entry[1]), is_package=entry[1])

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        source, is_package = self._sources[module.__name__]
        origin = self._origin(module.__name__, is_package)
        module.__file__ = origin
        exec(compile(source, origin, 'exec', dont_inherit=True), module.__dict__)

    def is_package(self, fullname):
        return self._sources[fullname][1]

    def get_source(self, fullname):
        # lets tracebacks show the embedded source lines
        return self._sources[fullname][0]


sys.meta_path.insert(0, EmbeddedImporter(SOURCES))

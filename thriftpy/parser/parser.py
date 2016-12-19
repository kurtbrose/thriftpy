# -*- coding: utf-8 -*-

"""
IDL Ref:
    https://thrift.apache.org/docs/idl
"""

from __future__ import absolute_import

import collections
import os
import sys
import types
import parsley
from .exc import ThriftParserError, ThriftGrammerError
from thriftpy._compat import urlopen, urlparse
from ..thrift import gen_init, TType, TPayload, TException


class ModuleLoader(object):
    '''
    Primary API for loading thrift files as modules.
    '''
    def __init__(self, include_dirs=('.',)):
        self.modules = {}
        self.samefile = getattr(os.path,'samefile', lambda f1, f2: os.stat(f1) == os.stat(f2))
        self.include_dirs = include_dirs

    def load(self, path, module_name):
        return self._load(path, True, module_name=module_name)

    def load_data(self, data, module_name):
        return self._load_data(data, module_name, False)

    def _load(self, path, load_includes, sofar=(), module_name=None):
        if not path.endswith('.thrift'):
            raise ParseError()  # ...
        if os.path.isabs(path):
            abs_path = path
        else:
            for base in self.include_dirs:
                abs_path = base + '/' + path
                if os.path.exists(abs_path):
                    break
            else:
                raise ParseError('could not find import {}'.format(path))
        if abs_path in sofar:
            cycle = sofar[sofar.index(abs_path):] + (abs_path,)
            path_to_cycle = sofar[:sofar.index(abs_path)]
            raise ImportError('circular import:\n{}\nvia:\n{}'.format(
                '->\n'.join(cycle), '->\n'.join(path_to_cycle)))
        with open(abs_path, 'rb') as f:
            data = f.read()
        if module_name is None:
            module_name = os.path.splitext(os.path.basename(abs_path))[0]  # remove '.thrift' from end
        return self._load_data(data, module_name, load_includes, sofar + (abs_path,))

    def _cant_load(self, path, *a, **kw):
        raise ParseError('cannot include sub-modules')

    def _load_data(self, data, module_name, load_includes, sofar=()):
        if module_name in self.modules:
            return self.modules[module_name]
        module = types.ModuleType(module_name)
        module.__thrift_meta__ = collections.defaultdict(list)
        if not load_includes:
            document = PARSER(data).Document(module, self._cant_load)
        else:
            document = PARSER(data).Document(
                module, lambda path: self._load(path, load_includes, sofar))
        self.modules[module_name] = module
        return self.modules[module_name]


MODULE_LOADER = ModuleLoader()


def parse(path, module_name=None, include_dirs=None, include_dir=None,
          enable_cache=True):
    """Parse a single thrift file to module object, e.g.::

        >>> from thriftpy.parser.parser import parse
        >>> note_thrift = parse("path/to/note.thrift")
        <module 'note_thrift' (built-in)>

    :param path: file path to parse, should be a string ending with '.thrift'.
    :param module_name: the name for parsed module, the default is the basename
                        without extension of `path`.
    :param include_dirs: directories to find thrift files while processing
                         the `include` directive, by default: ['.'].
    :param include_dir: directory to find child thrift files. Note this keyword
                        parameter will be deprecated in the future, it exists
                        for compatiable reason. If it's provided (not `None`),
                        it will be appended to `include_dirs`.
    :param enable_cache: if this is set to be `True`, parsed module will be
                         cached, this is enabled by default. If `module_name`
                         is provided, use it as cache key, else use the `path`.
    """
    if enable_cache and module_name in MODULE_LOADER.modules:
        return MODULE_LOADER.modules[module_name]

    cache_key = module_name or os.path.normpath(path)

    if include_dirs is not None:
        MODULE_LOADER.include_dirs = include_dirs
    if include_dir is not None:
        MODULE_LOADER.include_dirs.append(include_dir)

    if not path.endswith('.thrift'):
        raise ThriftParserError('Path should end with .thrift')

    url_scheme = urlparse(path).scheme
    if url_scheme == 'file':
        with open(urlparse(path).netloc + urlparse(path).path) as fh:
            data = fh.read()
    elif url_scheme == '':
        with open(path) as fh:
            data = fh.read()
    elif url_scheme in ('http', 'https'):
        data = urlopen(path).read()
        return MODULE_LOADER.load_data(data, module_name)
    else:
        raise ThriftParserError('ThriftPy does not support generating module '
                                'with path in protocol \'{}\''.format(
                                    url_scheme))

    if module_name is not None and not module_name.endswith('_thrift'):
        raise ThriftParserError('ThriftPy can only generate module with '
                                '\'_thrift\' suffix')

    if module_name is None:
        basename = os.path.basename(path)
        module_name = os.path.splitext(basename)[0]

    module = MODULE_LOADER.load(path, module_name)
    if not enable_cache:
        del MODULE_LOADER.modules[module_name]
    return module


def parse_fp(source, module_name, enable_cache=True):
    """Parse a file-like object to thrift module object, e.g.::

        >>> from thriftpy.parser.parser import parse_fp
        >>> with open("path/to/note.thrift") as fp:
                parse_fp(fp, "note_thrift")
        <module 'note_thrift' (built-in)>

    :param source: file-like object, expected to have a method named `read`.
    :param module_name: the name for parsed module, shoule be endswith
                        '_thrift'.
    :param enable_cache: if this is set to be `True`, parsed module will be
                         cached by `module_name`, this is enabled by default.
    """
    if not module_name.endswith('_thrift'):
        raise ThriftParserError('ThriftPy can only generate module with '
                                '\'_thrift\' suffix')

    if enable_cache and module_name in MODULE_LOADER.thrift_cache:
        return MODULE_LOADER.thrift_cache[module_name]

    if not hasattr(source, 'read'):
        raise ThriftParserError('Except `source` to be a file-like object with'
                                'a method named \'read\'')

    return MODULE_LOADER.load_data(source.read(), module_name, cache=enable_cache)


def _make_enum(name, kvs, module):
    attrs = {'__module__': module.__name__, '_ttype': TType.I32}
    cls = type(name, (object, ), attrs)

    _values_to_names = {}
    _names_to_values = {}

    if kvs:
        val = kvs[0][1]
        if val is None:
            val = -1
        for item in kvs:
            if item[1] is None:
                val = val + 1
            else:
                val = item[1]
        for key, val in kvs:
            setattr(cls, key, val)
            _values_to_names[val] = key
            _names_to_values[key] = val
    setattr(cls, '_VALUES_TO_NAMES', _values_to_names)
    setattr(cls, '_NAMES_TO_VALUES', _names_to_values)
    return cls


def _make_empty_struct(name, module, ttype=TType.STRUCT, base_cls=TPayload):
    attrs = {'__module__': module.__name__, '_ttype': ttype}
    return type(name, (base_cls, ), attrs)


def _fill_in_struct(cls, fields, _gen_init=True):
    thrift_spec = {}
    default_spec = []
    _tspec = {}

    for field in fields:
        if field.id in thrift_spec or field.name in _tspec:
            raise ThriftGrammerError(('\'%d:%s\' field identifier/name has '
                                      'already been used') % (field.id,
                                                              field.name))
        thrift_spec[field.id] = _ttype_spec(field.ttype, field.name, field.req)
        default_spec.append((field.name, field.default))
        _tspec[field.name] = field.req, field.ttype
    setattr(cls, 'thrift_spec', thrift_spec)
    setattr(cls, 'default_spec', default_spec)
    setattr(cls, '_tspec', _tspec)
    if _gen_init:
        gen_init(cls, thrift_spec, default_spec)
    return cls


def _make_struct(name, fields, module, ttype=TType.STRUCT, base_cls=TPayload,
                 _gen_init=True):
    cls = _make_empty_struct(name, module, ttype=ttype, base_cls=base_cls)
    return _fill_in_struct(cls, fields or (), _gen_init=_gen_init)


def _make_service(name, funcs, extends, module):
    if extends is None:
        extends = object

    attrs = {'__module__': module.__name__}
    cls = type(name, (extends, ), attrs)
    thrift_services = []

    for func in funcs:
        # args payload cls
        args_name = '%s_args' % func.name
        args_fields = func.fields
        args_cls = _make_struct(args_name, args_fields, module)
        setattr(cls, args_name, args_cls)
        # result payload cls
        result_name = '%s_result' % func.name
        result_cls = _make_struct(result_name, func.throws, module,
                                  _gen_init=False)
        setattr(result_cls, 'oneway', func.oneway)
        if func.ttype != TType.VOID:
            result_cls.thrift_spec[0] = _ttype_spec(func.ttype, 'success')
            result_cls.default_spec.insert(0, ('success', None))
        setattr(cls, result_name, result_cls)
        thrift_services.append(func.name)
    if extends is not None and hasattr(extends, 'thrift_services'):
        thrift_services.extend(extends.thrift_services)
    cls.thrift_services = thrift_services
    cls.thrift_extends = extends
    return cls


def _ttype_spec(ttype, name, required=False):
    if required is not False:
        required = (required == 'required')  # 'default' counts as 'optional'
    if isinstance(ttype, int):
        return ttype, name, required
    else:
        return ttype[0], name, ttype[1], required


def _get_ttype(inst, default_ttype=None):
    if hasattr(inst, '__dict__') and '_ttype' in inst.__dict__:
        return inst.__dict__['_ttype']
    return default_ttype


def _make_union(name, fields, module):
    cls = _make_empty_struct(name, module)
    return _fill_in_struct(cls, fields)


def _make_exception(name, fields, module):
    return _make_struct(name, fields, module, base_cls=TException)


def _add_definition(module, type, name, val, ttype):
    module.__thrift_meta__[type + 's'].append(val)
    module.__dict__[name] = val
    return type, name, val, ttype


def _add_include(module, path, loadf):
    included = loadf(path)
    module.__dict__[included.__name__] = included
    module.__thrift_meta__['includes'].append(included)
    return 'include', included


def _lookup_symbol(module, identifier):
    val = module
    for rel_name in identifier.split('.'):
        val = getattr(val, rel_name)
    return val


GRAMMAR = '''
Document :module :load_module = (brk Header(module load_module))*:hs (brk Definition(module))*:ds brk -> Document(hs, ds)
Header :module :load_module = <Include(module load_module) | Namespace>
Include :module :load_module = brk 'include' brk Literal:path -> Include(module, path, load_module)
Namespace =\
    brk 'namespace' brk <((NamespaceScope ('.' Identifier)?)| unsupported_namespacescope)>:scope brk\
        Identifier:name brk uri? -> 'namespace', scope, name
uri = '(' ws 'uri' ws '=' ws Literal:uri ws ')' -> uri
NamespaceScope = ('*' | 'cpp' | 'java' | 'py.twisted' | 'py' | 'perl' | 'rb' | 'cocoa' | 'csharp' |
                  'xsd' | 'c_glib' | 'js' | 'st' | 'go' | 'php' | 'delphi' | 'lua')
unsupported_namespacescope = Identifier
Definition :module = brk (Const(module) | Typedef(module) | Enum(module) | Struct(module) | Union(module) |
                           Exception(module) | Service(module)):defn -> Definition(module, *defn)
Const :module = 'const' brk FieldType(module):type brk Identifier:name brk '='\
    brk ConstValue(module):val brk ListSeparator? -> 'const', name, val, type
Typedef :module = 'typedef' brk DefinitionType(module):type brk Identifier:alias -> 'typedef', alias, type, None
Enum :module = 'enum' brk Identifier:name brk '{' enum_item*:vals '}'\
                -> 'enum', name, Enum(name, vals, module), None 
enum_item = brk Identifier:name brk ('=' brk IntConstant)?:value brk ListSeparator? brk -> name, value
Struct :module = 'struct' brk name_fields(module):nf brk immutable?\
                  -> 'struct', nf[0], Struct(nf[0], nf[1], module), None
Union :module = 'union' brk name_fields(module):nf -> 'union', nf[0], Union(nf[0], nf[1], module), None
Exception :module = 'exception' brk name_fields(module):nf\
                     -> 'exception', nf[0], Exception_(nf[0], nf[1], module), None
name_fields :module = Identifier:name brk '{' (brk Field(module))*:fields brk '}' -> name, fields
Service :module =\
    'service' brk Identifier:name brk ('extends' identifier_ref(module))?:extends '{' (brk Function(module))*:funcs brk '}'\
    -> 'service', name, Service(name, funcs, extends, module), None
Field :module = brk FieldID:id brk FieldReq?:req brk FieldType(module):ttype brk Identifier:name brk\
    ('=' brk ConstValue(module))?:default brk ListSeparator? -> Field(id, req, ttype, name, default)
FieldID = IntConstant:val ':' -> val
FieldReq = 'required' | 'optional' | !('default')
# Functions
Function :module = 'oneway'?:oneway brk FunctionType(module):ft brk Identifier:name '(' (brk Field(module)*):fs ')'\
     brk Throws(module)?:throws brk ListSeparator? -> Function(name, ft, fs, oneway, throws)
FunctionType :module = ('void' !(TType.VOID)) | FieldType(module)
Throws :module = 'throws' brk '(' (brk Field(module))*:fs ')' -> fs
# Types
FieldType :module = ContainerType(module) | BaseType | StructType(module)
DefinitionType :module = BaseType | ContainerType(module)
BaseType = ('bool' | 'byte' | 'i8' | 'i16' | 'i32' | 'i64' | 'double' | 'string' | 'binary'):ttype\
           -> BaseTType(ttype)
ContainerType :module = (MapType(module) | SetType(module) | ListType(module)):type brk immutable? -> type
MapType :module = 'map' CppType? brk '<' brk FieldType(module):keyt brk ',' brk FieldType(module):valt brk '>' -> TType.MAP, (keyt, valt)
SetType :module = 'set' CppType? brk '<' brk FieldType(module):valt brk '>' -> TType.SET, valt
ListType :module = 'list' brk '<' brk FieldType(module):valt brk '>' brk CppType? -> TType.LIST, valt
StructType :module = identifier_ref(module):name -> TType.STRUCT, name
CppType = 'cpp_type' Literal -> None
# Constant Values
ConstValue :module = DoubleConstant | IntConstant | ConstList(module) | ConstMap(module) | Literal | identifier_ref(module)
IntConstant = <('+' | '-')? Digit+>:val -> int(val)
DoubleConstant = <('+' | '-')? (Digit* '.' Digit+) | Digit+ (('E' | 'e') IntConstant)?>:val !(float(val)):fval\
                 -> fval if fval and fval % 1 else int(fval)  # favor integer representation if it is exact
ConstList :module = '[' (brk ConstValue(module):val ListSeparator? -> val)*:vals ']' -> vals
ConstMap :module = '{' (brk ConstValue(module):key ':' ConstValue(module):val ListSeparator? -> key, val)*:items '}' -> dict(items)
# Basic Definitions
Literal = (('"' <(~'"' anything)*>:val '"') | ("'" <(~"'" anything)*>:val "'")) -> val
Identifier = not_reserved <(Letter | '_') (Letter | Digit | '.' | '_')*>
identifier_ref :module = Identifier:val -> IdentifierRef(module, val)  # unresolved reference
ListSeparator = ',' | ';'
Letter = letter  # parsley built-in
Digit = digit  # parsley built-in
Comment = cpp_comment | c_comment
brk = <(' ' | '\t' | '\n' | '\r' | c_comment | cpp_comment)*>
cpp_comment = '//' <('\\\n' | (~'\n' anything))*>
c_comment = '/*' <(~'*/' anything)*>:body '*/' -> body
immutable = '(' brk 'python.immutable' brk '=' brk '""' brk ')'
Reserved = ('__CLASS__' | '__DIR__' | '__FILE__' | '__FUNCTION__' | '__LINE__' | '__METHOD__' |
            '__NAMESPACE__' | 'abstract' | 'alias' | 'and' | 'args' | 'as' | 'assert' | 'BEGIN' |
            'begin' | 'binary' | 'bool' | 'break' | 'byte' | 'case' | 'catch' | 'class' | 'clone' |
            'const' | 'continue' | 'declare' | 'def' | 'default' | 'del' | 'delete' | 'do' |
            'double' | 'dynamic' | 'elif' | 'else' | 'elseif' | 'elsif' | 'END' | 'end' |
            'enddeclare' | 'endfor' | 'endforeach' | 'endif' | 'endswitch' | 'endwhile' | 'ensure' |
            'enum' | 'except' | 'exception' | 'exec' | 'extends' | 'finally' | 'float' | 'for' |
            'foreach' | 'from' | 'function' | 'global' | 'goto' | 'i16' | 'i32' | 'i64' | 'if' |
            'implements' | 'import' | 'in' | 'include' | 'inline' | 'instanceof' | 'interface' |
            'is' | 'lambda' | 'list' | 'map' | 'module' | 'namespace' | 'native' | 'new' | 'next' |
            'nil' | 'not' | 'oneway' | 'optional' | 'or' | 'pass' | 'print' | 'private' |
            'protected' | 'public' | 'public' | 'raise' | 'redo' | 'register' | 'required' |
            'rescue' | 'retry' | 'return' | 'self' | 'service' | 'set' | 'sizeof' | 'static' |
            'string' | 'struct' | 'super' | 'switch' | 'synchronized' | 'then' | 'this' |
            'throw' | 'throws' | 'transient' | 'try' | 'typedef' | 'undef' | 'union' | 'union' |
            'unless' | 'unsigned' | 'until' | 'use' | 'var' | 'virtual' | 'void' | 'volatile' |
            'when' | 'while' | 'with' | 'xor' | 'yield')
not_reserved = ~(Reserved (' ' | '\t' | '\n'))
'''


BASE_TYPE_MAP = {
    'bool': TType.BOOL,
    'byte': TType.BYTE,
    'i8': TType.BYTE,
    'i16': TType.I16,
    'i32': TType.I32,
    'i64': TType.I64,
    'double': TType.DOUBLE,
    'string': TType.STRING,
    'binary': TType.BINARY
}


PARSER = parsley.makeGrammar(
    GRAMMAR, 
    {
        'Document': collections.namedtuple('Document', 'headers definitions'),
        'Include': _add_include,
        'Definition': _add_definition,
        'Enum': _make_enum,
        'Struct': _make_struct,
        'Union': _make_union,
        'Exception_': _make_exception,
        'Service': _make_service,
        'Function': collections.namedtuple('Function', 'name ttype fields oneway throws'),
        'Field': collections.namedtuple('Field', 'id req ttype name default'),
        'IdentifierRef': _lookup_symbol,
        'BaseTType': BASE_TYPE_MAP.get,
        'TType': TType
    }
)


class ParseError(ThriftGrammerError): pass


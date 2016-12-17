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

    def load(self, path):
        return self._load(path, True)

    def load_data(self, data, module_name):
        return self._load_data(data, module_name, False)

    def _load(self, path, load_includes, sofar=()):
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
        with open(abs_path, 'rb') as f:
            data = f.read()
        module_name = os.path.basename(abs_path).replace('.thrift', '_thrift')
        return self._load_data(data, module_name)

    def _load_data(self, data, module_name):
        if module_name in self.modules:
            return self.modules[module_name]
        module = types.ModuleType(module_name)
        document = PARSER(data).Document(module_name)
        for header in document.headers:
            if header[0] == 'include':
                if not load_includes:
                    raise ParseError('cannot include sub-module')
                included = self._load(header[1], sofar + (path,))
            if header[0] == 'namespace':
                pass  # namespace is currently ignored
        module.__thrift_meta__ = collections.defaultdict(list)
        def lookup_symbol(dot_name):  # TODO: lookup non-local symbols
            val = module
            for rel_name in dot_name.split('.'):
                val = getattr(val, rel_name)
            return val
        for defn in document.definitions:
            # intercept const referencing identifiers and resolve
            if defn.type == 'const' and type(defn.val) is ThriftIdentifier:
                val = lookup_symbol(defn.val)
            else:
                val = defn.val
            setattr(module, defn.name, val)
            module.__thrift_meta__[defn.type + 's'].append(val)
            # resolve identifier references in other types
            if defn.type == 'service':
                service = defn.val
                if service.thrift_extends:
                    service.__base__ = lookup_symbol(service.thrift_extends)
                # check each of the arg and result structs in the service for identifier refs
                for key, struct in service.__dict__.items():
                    if not isinstance(struct, TPayload):
                        continue
                    for i in range(len(struct.default_spec)):
                        name, default = struct.default_spec[i]
                        if type(default) is ThriftIdentifier:
                            struct.default_spec[i] = name, lookup_symbol(default)
                    if key.endswith('_result'):
                        gen_init(struct, struct.thrift_spec, struct.default_spec)
                if service.thrift_extends:
                    service.thrift_services.extend()  # TODO: resolution order or something here
            elif defn.type in ('struct', 'union', 'exception'):
                # resolve field defaults if they are identifiers
                struct = defn.val
                for i in range(len(struct.default_spec)):
                    name, default = struct.default_spec[i]
                    if type(default) is ThriftIdentifier:
                        struct.default_spec[i] = name, lookup_symbol(default)
                gen_init(struct, struct.thrift_spec, struct.default_spec)
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

    module = MODULE_LOADER.load(path)
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


def _make_enum(name, kvs, modname):
    attrs = {'__module__': modname, '_ttype': TType.I32}
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


def _make_empty_struct(name, modname, ttype=TType.STRUCT, base_cls=TPayload):
    attrs = {'__module__': modname, '_ttype': ttype}
    return type(name, (base_cls, ), attrs)


def _fill_in_struct(cls, fields):
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
    return cls


def _make_struct(name, fields, modname, ttype=TType.STRUCT, base_cls=TPayload):
    cls = _make_empty_struct(name, modname, ttype=ttype, base_cls=base_cls)
    return _fill_in_struct(cls, fields or ())


def _make_service(name, funcs, extends, modname):
    attrs = {'__module__': modname}
    cls = type(name, (object, ), attrs)
    thrift_services = []

    for func in funcs:
        # args payload cls
        args_name = '%s_args' % func.name
        args_fields = func.fields
        args_cls = _make_struct(args_name, args_fields, modname)
        setattr(cls, args_name, args_cls)
        # result payload cls
        result_name = '%s_result' % func.name
        result_cls = _make_struct(result_name, func.throws, modname)
        setattr(result_cls, 'oneway', func.oneway)
        if func.ttype != TType.VOID:
            result_cls.thrift_spec[0] = _ttype_spec(func.ttype, 'success')
            result_cls.default_spec.insert(0, ('success', None))
        setattr(cls, result_name, result_cls)
        thrift_services.append(func.name)
    cls.thrift_services = thrift_services
    cls.thrift_extends = extends
    return cls


def _ttype_spec(ttype, name, required=False):
    if isinstance(ttype, int):
        return ttype, name, required
    else:
        return ttype[0], name, ttype[1], required


def _get_ttype(inst, default_ttype=None):
    if hasattr(inst, '__dict__') and '_ttype' in inst.__dict__:
        return inst.__dict__['_ttype']
    return default_ttype


def _make_union(name, fields, modname):
    cls = _make_empty_struct(name, modname)
    return _fill_in_struct(cls, fields)


def _make_exception(name, fields, modname):
    return _make_struct(name, fields, modname, base_cls=TException)


GRAMMAR = '''
Document :modname = (brk Header)*:hs (brk Definition(modname))*:ds brk -> Document(hs, ds)
Header = <Include | Namespace>
Include = brk 'include' brk Literal:path -> 'include', path
Namespace =\
    brk 'namespace' brk <((NamespaceScope ('.' Identifier)?)| unsupported_namespacescope)>:scope brk\
        Identifier:name brk uri? -> 'namespace', scope, name
uri = '(' ws 'uri' ws '=' ws Literal:uri ws ')' -> uri
NamespaceScope = ('*' | 'cpp' | 'java' | 'py.twisted' | 'py' | 'perl' | 'rb' | 'cocoa' | 'csharp' |
                  'xsd' | 'c_glib' | 'js' | 'st' | 'go' | 'php' | 'delphi' | 'lua')
unsupported_namespacescope = Identifier
Definition :modname = brk (Const | Typedef | Enum(modname) | Struct(modname) | Union(modname) |
                           Exception(modname) | Service(modname)):defn -> Definition(*defn)
Const = 'const' brk FieldType:type brk Identifier:name brk '='\
    brk ConstValue:val brk ListSeparator? -> 'const', name, val, type
Typedef = 'typedef' brk DefinitionType:type brk Identifier:alias -> 'typedef', alias, type, None
Enum :modname = 'enum' brk Identifier:name brk '{' enum_item*:vals '}'\
                -> 'enum', name, Enum(name, vals, modname), None 
enum_item = brk Identifier:name brk ('=' brk IntConstant)?:value brk ListSeparator? brk -> name, value
Struct :modname = 'struct' brk name_fields:nf brk immutable?\
                  -> 'struct', nf[0], Struct(nf[0], nf[1], modname), None
Union :modname = 'union' brk name_fields:nf -> 'union', nf[0], Union(nf[0], nf[1], modname), None
Exception :modname = 'exception' brk name_fields:nf\
                     -> 'exception', nf[0], Exception_(nf[0], nf[1], modname), None
name_fields = Identifier:name brk '{' (brk Field)*:fields brk '}' -> name, fields
Service :modname =\
    'service' brk Identifier:name brk ('extends' identifier_ref)?:extends '{' (brk Function)*:funcs brk '}'\
    -> 'service', name, Service(name, funcs, extends, modname), None
Field = brk FieldID:id brk FieldReq?:req brk FieldType:ttype brk Identifier:name brk\
    ('=' brk ConstValue)?:default brk ListSeparator? -> Field(id, req, ttype, name, default)
FieldID = IntConstant:val ':' -> val
FieldReq = 'required' | 'optional' | !('default')
# Functions
Function = 'oneway'?:oneway brk FunctionType:ft brk Identifier:name '(' (brk Field*):fs ')'\
     brk Throws?:throws brk ListSeparator? -> Function(name, ft, fs, oneway, throws)
FunctionType = ('void' !(TType.VOID)) | FieldType
Throws = 'throws' brk '(' (brk Field)*:fs ')' -> fs
# Types
FieldType = ContainerType | BaseType | StructType
DefinitionType = BaseType | ContainerType
BaseType = ('bool' | 'byte' | 'i8' | 'i16' | 'i32' | 'i64' | 'double' | 'string' | 'binary'):ttype\
           -> BaseTType(ttype)
ContainerType = (MapType | SetType | ListType):type brk immutable? -> type
MapType = 'map' CppType? brk '<' brk FieldType:keyt brk ',' brk FieldType:valt brk '>' -> TType.MAP, (keyt, valt)
SetType = 'set' CppType? brk '<' brk FieldType:valt brk '>' -> TType.SET, valt
ListType = 'list' brk '<' brk FieldType:valt brk '>' brk CppType? -> TType.LIST, valt
StructType = Identifier:name -> TType.STRUCT, name
CppType = 'cpp_type' Literal -> None
# Constant Values
ConstValue = DoubleConstant | IntConstant | ConstList | ConstMap | Literal | identifier_ref
IntConstant = <('+' | '-')? Digit+>:val -> int(val)
DoubleConstant = <('+' | '-')? (Digit* '.' Digit+) | Digit+ (('E' | 'e') IntConstant)?>:val !(float(val)):fval\
                 -> fval if fval and fval % 1 else int(fval)  # favor integer representation if it is exact
ConstList = '[' (brk ConstValue:val ListSeparator? -> val)*:vals ']' -> vals
ConstMap = '{' (brk ConstValue:key ':' ConstValue:val ListSeparator? -> key, val)*:items '}' -> dict(items)
# Basic Definitions
Literal = (('"' <(~'"' anything)*>:val '"') | ("'" <(~"'" anything)*>:val "'")) -> val
Identifier = not_reserved <(Letter | '_') (Letter | Digit | '.' | '_')*>
identifier_ref = Identifier:val -> IdentifierRef(val)  # unresolved reference
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


class ThriftIdentifier(str):
    'marker class to separate thrift identifiers from actual string values'


PARSER = parsley.makeGrammar(
    GRAMMAR, 
    {
        'Document': collections.namedtuple('Document', 'headers definitions'),
        'Definition': collections.namedtuple('Definition', 'type name val ttype'),
        'Enum': _make_enum,
        'Struct': _make_struct,
        'Union': _make_union,
        'Exception_': _make_exception,
        'Service': _make_service,
        'Function': collections.namedtuple('Function', 'name ttype fields oneway throws'),
        'Field': collections.namedtuple('Field', 'id req ttype name default'),
        'IdentifierRef': ThriftIdentifier,
        'BaseTType': BASE_TYPE_MAP.get,
        'TType': TType
    }
)


class ParseError(ThriftGrammerError): pass


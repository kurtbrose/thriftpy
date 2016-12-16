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
from .exc import ThriftParserError, ThriftGrammerError
from thriftpy._compat import urlopen, urlparse
from ..thrift import gen_init, TType, TPayload, TException


class ModuleLoader(object):
    def __init__(self):
        self.modules = {}

    def load(self, path):
        if modname not in self.modules:
            self.modules[modname] = PARSER(modname, self.load).Document()
        return self.modules[modname] 


def parse(path, module_name=None, include_dirs=None, include_dir=None,
          lexer=None, parser=None, enable_cache=True):
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
    :param lexer: ply lexer to use, if not provided, `parse` will new one.
    :param parser: ply parser to use, if not provided, `parse` will new one.
    :param enable_cache: if this is set to be `True`, parsed module will be
                         cached, this is enabled by default. If `module_name`
                         is provided, use it as cache key, else use the `path`.
    """
    if os.name == 'nt' and sys.version_info < (3, 2):
        os.path.samefile = lambda f1, f2: os.stat(f1) == os.stat(f2)

    # dead include checking on current stack
    for thrift in thrift_stack:
        if thrift.__thrift_file__ is not None and \
                os.path.samefile(path, thrift.__thrift_file__):
            raise ThriftParserError('Dead including on %s' % path)

    global thrift_cache

    cache_key = module_name or os.path.normpath(path)

    if enable_cache and cache_key in thrift_cache:
        return thrift_cache[cache_key]

    if lexer is None:
        lexer = lex.lex()
    if parser is None:
        parser = yacc.yacc(debug=False, write_tables=0)

    global include_dirs_

    if include_dirs is not None:
        include_dirs_ = include_dirs
    if include_dir is not None:
        include_dirs_.append(include_dir)

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

    thrift = types.ModuleType(module_name)
    setattr(thrift, '__thrift_file__', path)
    thrift_stack.append(thrift)
    lexer.lineno = 1
    parser.parse(data)
    thrift_stack.pop()

    if enable_cache:
        thrift_cache[cache_key] = thrift
    return thrift


def parse_fp(source, module_name, lexer=None, parser=None, enable_cache=True):
    """Parse a file-like object to thrift module object, e.g.::

        >>> from thriftpy.parser.parser import parse_fp
        >>> with open("path/to/note.thrift") as fp:
                parse_fp(fp, "note_thrift")
        <module 'note_thrift' (built-in)>

    :param source: file-like object, expected to have a method named `read`.
    :param module_name: the name for parsed module, shoule be endswith
                        '_thrift'.
    :param lexer: ply lexer to use, if not provided, `parse` will new one.
    :param parser: ply parser to use, if not provided, `parse` will new one.
    :param enable_cache: if this is set to be `True`, parsed module will be
                         cached by `module_name`, this is enabled by default.
    """
    if not module_name.endswith('_thrift'):
        raise ThriftParserError('ThriftPy can only generate module with '
                                '\'_thrift\' suffix')

    if enable_cache and module_name in thrift_cache:
        return thrift_cache[module_name]

    if not hasattr(source, 'read'):
        raise ThriftParserError('Except `source` to be a file-like object with'
                                'a method named \'read\'')

    if lexer is None:
        lexer = lex.lex()
    if parser is None:
        parser = yacc.yacc(debug=False, write_tables=0)

    data = source.read()

    thrift = types.ModuleType(module_name)
    setattr(thrift, '__thrift_file__', None)
    thrift_stack.append(thrift)
    lexer.lineno = 1
    parser.parse(data)
    thrift_stack.pop()

    if enable_cache:
        thrift_cache[module_name] = thrift
    return thrift


def _add_thrift_meta(key, val):
    thrift = thrift_stack[-1]

    if not hasattr(thrift, '__thrift_meta__'):
        meta = collections.defaultdict(list)
        setattr(thrift, '__thrift_meta__',  meta)
    else:
        meta = getattr(thrift, '__thrift_meta__')

    meta[key].append(val)


def _make_enum(name, kvs):
    attrs = {'__module__': thrift_stack[-1].__name__, '_ttype': TType.I32}
    cls = type(name, (object, ), attrs)

    _values_to_names = {}
    _names_to_values = {}

    if kvs:
        val = kvs[0][1]
        if val is None:
            val = -1
        for item in kvs:
            if item[1] is None:
                item[1] = val + 1
            val = item[1]
        for key, val in kvs:
            setattr(cls, key, val)
            _values_to_names[val] = key
            _names_to_values[key] = val
    setattr(cls, '_VALUES_TO_NAMES', _values_to_names)
    setattr(cls, '_NAMES_TO_VALUES', _names_to_values)
    return cls


def _make_empty_struct(name, ttype=TType.STRUCT, base_cls=TPayload):
    attrs = {'__module__': thrift_stack[-1].__name__, '_ttype': ttype}
    return type(name, (base_cls, ), attrs)


def _fill_in_struct(cls, fields, _gen_init=True):
    thrift_spec = {}
    default_spec = []
    _tspec = {}

    for field in fields:
        if field[0] in thrift_spec or field[3] in _tspec:
            raise ThriftGrammerError(('\'%d:%s\' field identifier/name has '
                                      'already been used') % (field[0],
                                                              field[3]))
        ttype = field[2]
        thrift_spec[field[0]] = _ttype_spec(ttype, field[3], field[1])
        default_spec.append((field[3], field[4]))
        _tspec[field[3]] = field[1], ttype
    setattr(cls, 'thrift_spec', thrift_spec)
    setattr(cls, 'default_spec', default_spec)
    setattr(cls, '_tspec', _tspec)
    if _gen_init:
        gen_init(cls, thrift_spec, default_spec)
    return cls


def _make_struct(name, fields, ttype=TType.STRUCT, base_cls=TPayload,
                 _gen_init=True):
    cls = _make_empty_struct(name, ttype=ttype, base_cls=base_cls)
    return _fill_in_struct(cls, fields, _gen_init=_gen_init)


def _make_service(name, funcs, extends):
    if extends is None:
        extends = object

    attrs = {'__module__': thrift_stack[-1].__name__}
    cls = type(name, (extends, ), attrs)
    thrift_services = []

    for func in funcs:
        func_name = func[2]
        # args payload cls
        args_name = '%s_args' % func_name
        args_fields = func[3]
        args_cls = _make_struct(args_name, args_fields)
        setattr(cls, args_name, args_cls)
        # result payload cls
        result_name = '%s_result' % func_name
        result_type = func[1]
        result_throws = func[4]
        result_oneway = func[0]
        result_cls = _make_struct(result_name, result_throws,
                                  _gen_init=False)
        setattr(result_cls, 'oneway', result_oneway)
        if result_type != TType.VOID:
            result_cls.thrift_spec[0] = _ttype_spec(result_type, 'success')
            result_cls.default_spec.insert(0, ('success', None))
        gen_init(result_cls, result_cls.thrift_spec, result_cls.default_spec)
        setattr(cls, result_name, result_cls)
        thrift_services.append(func_name)
    if extends is not None and hasattr(extends, 'thrift_services'):
        thrift_services.extend(extends.thrift_services)
    setattr(cls, 'thrift_services', thrift_services)
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


GRAMMAR = '''
Document :modname = (brk Header)*:hs (brk Definition(modname))*:ds brk -> Document(hs, ds)
Header = <Include | Namespace>
Include = brk 'include' brk Literal:path -> 'include', path
Namespace = brk 'namespace' brk <((NamespaceScope ('.' Identifier)?)| unsupported_namespacescope)>:scope brk Identifier:name brk uri? -> 'namespace', scope, name
uri = '(' ws 'uri' ws '=' ws Literal:uri ws ')' -> uri
NamespaceScope = '*' | 'cpp' | 'java' | 'py.twisted' | 'py' | 'perl' | 'rb' | 'cocoa' | 'csharp' | 'xsd' | 'c_glib' | 'js' | 'st' | 'go' | 'php' | 'delphi' | 'lua'
unsupported_namespacescope = Identifier
Definition :modname = brk (Const | Typedef | Enum(modname) | Struct(modname) | Union(modname) | Exception(modname) | Service(modname))
Const = 'const' brk FieldType:type brk Identifier:name brk '=' brk ConstValue:val brk ListSeparator? -> 'const', type, name, val
Typedef = 'typedef' brk DefinitionType:type brk Identifier:alias -> 'typedef', type, alias
Enum :modname = 'enum' brk Identifier:name brk '{' enum_item*:vals '}' -> Enum(name, vals, modname) 
enum_item = brk Identifier:name brk ('=' brk IntConstant)?:value brk ListSeparator? brk -> name, value
Struct :modname = 'struct' brk name_fields:nf brk immutable? -> Struct(nf[0], nf[1], modname)
Union :modname = 'union' brk name_fields:nf -> Union(nf[0], nf[1], modname)
Exception :modname = 'exception' brk name_fields:nf -> Exception_(nf[0], nf[1], modname)
name_fields = Identifier:name brk '{' (brk Field)*:fields brk '}' -> name, fields
Service :modname = 'service' brk Identifier:name brk ('extends' Identifier)?:extends '{' (brk Function)*:funcs brk '}' -> Service(name, funcs, extends, modname)
Field = brk FieldID:id brk FieldReq?:req brk FieldType:ttype brk Identifier:name brk ('=' brk ConstValue)?:default brk ListSeparator? -> Field(id, req, ttype, name, default)
FieldID = IntConstant:val ':' -> val
FieldReq = 'required' | 'optional' | !('default')
# Functions
Function = 'oneway'?:oneway brk FunctionType:ft brk Identifier:name '(' (brk Field*):fs ')' brk Throws?:throws brk ListSeparator? -> Function(name, ft, fs, oneway, throws)
FunctionType = ('void' !(TType.VOID)) | FieldType
Throws = 'throws' '(' (brk Field)*:fs ')' -> fs
# Types
FieldType = ContainerType | BaseType | StructType
DefinitionType = BaseType | ContainerType
BaseType = ('bool' | 'byte' | 'i8' | 'i16' | 'i32' | 'i64' | 'double' | 'string' | 'binary'):ttype -> BaseTType(ttype)
ContainerType = (MapType | SetType | ListType):type brk immutable? -> type
MapType = 'map' CppType? brk '<' brk FieldType:keyt brk ',' brk FieldType:valt brk '>' -> TType.MAP, (keyt, valt)
SetType = 'set' CppType? brk '<' brk FieldType:valt brk '>' -> TType.SET, valt
ListType = 'list' brk '<' brk FieldType:valt brk '>' brk CppType? -> TType.LIST, valt
StructType = Identifier:name -> TType.STRUCT, name
CppType = 'cpp_type' Literal -> None
# Constant Values
ConstValue = IntConstant | DoubleConstant | ConstList | ConstMap | Literal | Identifier
IntConstant = <('+' | '-')? Digit+>:val -> int(val)
DoubleConstant = <('+' | '-')? (Digit* '.' Digit+) | Digit+ (('E' | 'e') IntConstant)?> -> float(val)
ConstList = '[' (ConstValue:val ListSeparator? -> val)*:vals ']' -> vals
ConstMap = '{' (ConstValue:key ':' ConstValue:val ListSeparator? -> key, val)*:items '}' -> dict(items)
# Basic Definitions
Literal = (('"' <(~'"' anything)*>:val '"') | ("'" <(~"'" anything)*>:val "'")) -> val
Identifier = not_reserved <(Letter | '_') (Letter | Digit | '.' | '_')*>
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
        'Enum': _make_enum,
        'Struct': _make_struct,
        'Union': _make_union,
        'Exception_': _make_exception,
        'Service': _make_service,
        'Function': collections.namedtuple('Function', 'name ttype fields oneway throws'),
        'Field': collections.namedtuple('Field', 'id req ttype name default'),
        'BaseTType': BASE_TYPE_MAP.get,
        'TType': TType
    }
)
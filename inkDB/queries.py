#!/usr/bin/env python3

## queries

import re
import sys

from .utils import catch_warning, freeze

__all__ = ('Query', 'where')


def is_sequence(obj):
    return hasattr(obj, '__iter__')


class QueryImpl(object):
    def __init__(self, test, hashval):
        self.test = test
        self.hashval = hashval

    def __call__(self, value):
        return self.test(value)

    def __hash__(self):
        return hash(self.hashval)

    def __repr__(self):
        return 'QueryImpl{0}'.format(self.hashval)

    def __eq__(self, other):
        return self.hashval == other.hashval

    def __and__(self, other):
        return QueryImpl(lambda value: self(value) and other(value),
                         ('and', frozenset([self.hashval, other.hashval])))

    def __or__(self, other):
        return QueryImpl(lambda value: self(value) or other(value),
                         ('or', frozenset([self.hashval, other.hashval])))

    def __invert__(self):
        return QueryImpl(lambda value: not self(value),
                         ('not', self.hashval))


class Query(object):
    def __init__(self):
        self._path = []

    def __getattr__(self, item):
        query = Query()
        query._path = self._path + [item]

        return query

    __getitem__ = __getattr__

    def _generate_test(self, test, hashval):
        if not self._path:
            raise ValueError('Query has no path')

        def impl(value):
            try:
                for part in self._path:
                    value = value[part]
            except (KeyError, TypeError):
                return False
            else:
                return test(value)

        return QueryImpl(impl, hashval)

    def __eq__(self, rhs):
        if sys.version_info <= (3, 0): 
            def test(value):
                with catch_warning(UnicodeWarning):
                    try:
                        return value == rhs
                    except UnicodeWarning:
                        if isinstance(value, str):
                            return value.decode('utf-8') == rhs
                        elif isinstance(rhs, str):
                            return value == rhs.decode('utf-8')

        else: 
            def test(value):
                return value == rhs

        return self._generate_test(lambda value: test(value),
                                   ('==', tuple(self._path), freeze(rhs)))

    def __ne__(self, rhs):
        return self._generate_test(lambda value: value != rhs,
                                   ('!=', tuple(self._path), freeze(rhs)))

    def __lt__(self, rhs):
        return self._generate_test(lambda value: value < rhs,
                                   ('<', tuple(self._path), rhs))

    def __le__(self, rhs):
        return self._generate_test(lambda value: value <= rhs,
                                   ('<=', tuple(self._path), rhs))

    def __gt__(self, rhs):
        return self._generate_test(lambda value: value > rhs,
                                   ('>', tuple(self._path), rhs))

    def __ge__(self, rhs):
        return self._generate_test(lambda value: value >= rhs,
                                   ('>=', tuple(self._path), rhs))

    def exists(self):
        return self._generate_test(lambda _: True,
                                   ('exists', tuple(self._path)))

    def matches(self, regex):
        return self._generate_test(lambda value: re.match(regex, value),
                                   ('matches', tuple(self._path), regex))

    def matches_ignore_case(self, regex):
        regex = regex.lower()
        return self._generate_test(lambda value: re.match(regex, value.lower()),
                                   ('matches', tuple(self._path), regex))                                   

    def search(self, regex):
        return self._generate_test(lambda value: re.search(regex, value),
                                   ('search', tuple(self._path), regex))

    def search_ignore_case(self, regex):
        regex = regex.lower()
        return self._generate_test(lambda value: re.search(regex, value.lower()),
                                   ('search', tuple(self._path), regex))                                   

    def test(self, func, *args):
        return self._generate_test(lambda value: func(value, *args),
                                   ('test', tuple(self._path), func, args))

    def any(self, cond):
        if callable(cond):
            def _cmp(value):
                return is_sequence(value) and any(cond(e) for e in value)

        else:
            def _cmp(value):
                return is_sequence(value) and any(e in cond for e in value)

        return self._generate_test(lambda value: _cmp(value),
                                   ('any', tuple(self._path), freeze(cond)))

    def all(self, cond):
        if callable(cond):
            def _cmp(value):
                return is_sequence(value) and all(cond(e) for e in value)

        else:
            def _cmp(value):
                return is_sequence(value) and all(e in value for e in cond)

        return self._generate_test(lambda value: _cmp(value),
                                   ('all', tuple(self._path), freeze(cond)))


def where(key):
    return Query()[key]
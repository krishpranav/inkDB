#!/usr/bin/env python3 

# operations
def delete(field):
    def transform(element):
        del element[field]

    return transform


def increment(field):
    def transform(element):
        element[field] += 1

    return transform


def decrement(field):
    def transform(element):
        element[field] -= 1

    return transform
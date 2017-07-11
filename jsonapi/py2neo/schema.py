#!/usr/bin/env python3

# The MIT License (MIT)
#
# Copyright (c) 2016 Benedikt Schmitt
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
jsonapi.py2neo.schema
=========================

The *py-jsonapi* schema for *py2neo* models.
"""

# std
import logging

# third party
import py2neo

# local
import jsonapi


__all__ = [
    "Constructor",
    "Attribute",
    "IDAttribute",
    "ToOneRelationship",
    "ToManyRelationship",
    "Schema"
]


nLOG = logging.getLogger(__file__)


class Constructor(jsonapi.base.schema.Constructor):
    """
    """
    def __init__(self, resource_class):
        super.__init__(resource_class=resource_class)

    def create(self, **kwargs):
        # for k, v in kwargs:
        #     print(k, v)
        return None

class Attribute(jsonapi.base.schema.Attribute):
    """
    Wraps py2neo model attributes

    :arg name:
        The name of the py2neo property
    :arg resource_class:
        The py2neo model class
    :arg neo_field:
        An py2neo Property, Label or Related* object
    """
    def __init__(self, name, resource_class, neo_field):
        super().__init__(name=name)
        self.resource_class = resource_class
        self.neo_field = neo_field

    def get(self, resource):
        return self.neo_field.__get__(resource, None)

    # def set(self):
    #     pass


class IDAttribute(jsonapi.base.schema.IDAttribute):
    """
    Wraps an py2neo primary key. We only allow read it, but not change.

    :arg str name:
        The name of the id field.
    :arg resource_class:
        The py2neo model class
    :arg neo_field:
        The py2neo id field of the resource class
    """

    def __init__(self, name, resource_class, neo_field):
        super().__init__(name=name)
        self.resource_class = resource_class
        self.neo_field = neo_field

    def get(self, resource):
        """
        We use the Inspector for :attr:`resource_class` to get the primary key
        for the resource.
        """
        if resource.__primarykey__ == '__id__':
            return resource.__primaryvalue__
        return str(self.neo_field.__get__(resource, None))

    def set(self, ):
        raise AttributeError


class ToOneRelationship(jsonapi.base.schema.ToOneRelationship):
    """
    To one relationship here
    """
    pass


class ToManyRelationship(jsonapi.base.schema.ToManyRelationship):
    """
    Wraps an py2neo to-many relationship.

    :arg str name:
        The name of the py2neo relationship
    :arg resource_class:
        The py2neo model
    :arg relobj:
        The relationship defined on the model
    """

    def __init__(self, name, resource_class, relobj):
        super().__init__(name=name)
        self.resource_class = resource_class
        self.relobj = relobj

    def get(self, resource):
        return self.relobj.__get__(resource, None)


class Schema(jsonapi.base.schema.Schema):
    """
    This schema subclass finds also py2neo attributes and relationships
    defined on the resource class.

    :arg resource_class:
        The py2neo model
    :arg str typename:
        The typename of the resources in the JSONapi. If not given, it is
        derived from the resource class.
    """

    def __init__(self, resource_class, typename=None):
        """
        """
        super().__init__(resource_class, typename)
        self.find_py2neo_markers()

    def find_py2neo_markers(self):
        """
        """
        for name, field in vars(self.resource_class).items():

            if name.startswith("_"):
                continue
            if isinstance(field, py2neo.ogm.Property):
                # to attrs
                attr = Attribute(name, self.resource_class, field)
                self.attributes[attr.name] = attr
                self.fields.add(attr.name)
            if (isinstance(field, py2neo.ogm.Related) and
               not isinstance(field, py2neo.ogm.RelatedFrom)):
                # to relationships
                rel = ToManyRelationship(name, self.resource_class, field)
                self.relationships[rel.name] = rel
                self.fields.add(rel.name)

        # Use the primarykey of the resource_class, if no id marker is set.
        self.id_attribute = IDAttribute(name, self.resource_class, field)

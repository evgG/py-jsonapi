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
jsonapi.py2neo.database
===========================

The database adapter for *py2neo*.
"""

# std
# from itertools import groupby

# local
import jsonapi
from . import schema


__all__ = [
    "Database",
    "Session"
]


class Database(jsonapi.base.database.Database):
    """
    This adapter must be chosen for py2neo models
    and without __primarykey__ in models.
    """

    def __init__(self, sessionmaker=None, api=None):
        super().__init__(api=api)

        if sessionmaker is None and api is not None:
            sessionmaker = self.api.settings["py2neo_sessionmaker"]
        self.sessionmaker = sessionmaker

    def session(self):
        return Session(self.api, self.sessionmaker)


class Session(jsonapi.base.database.Session):
    """
    Session for py2neo
    """

    def __init__(self, api, py2neo_session):
        self.api = api
        self.py2neo_session = py2neo_session

    def _build_filter_criterion(self, schema_, filters):
        """
        Builds a dictionary, which can be used inside a document's *objects()*
        method to filter the resources by the *japi_filters* dictionary.

        :arg jsonapi.py2neo.schema.Schema schema_:
        :arg filters:
        """
        d = dict()
        for fieldname, filtername, value in filters:

            # We only allow filtering for py2neo attributes.
            attribute = schema_.attributes.get(fieldname)
            if not isinstance(attribute, schema.Attribute):
                raise jsonapi.base.errors.UnfilterableField(filtername,
                                                            fieldname)

            if filtername == "eq":
                d[attribute.name] = value
            elif filtername == "ne":
                d[attribute.name + "__ne"] = value
            elif filtername == "lt":
                d[attribute.name + "__lt"] = value
            elif filtername == "lte":
                d[attribute.name + "__lte"] = value
            elif filtername == "gt":
                d[attribute.name + "__gt"] = value
            elif filtername == "gte":
                d[attribute.name + "__gte"] = value
            elif filtername == "in":
                d[attribute.name + "__in"] = value
            elif filtername == "nin":
                d[attribute.name + "__nin"] = value
            elif filtername == "all":
                d[attribute.name + "__all"] = value
            elif filtername == "size":
                d[attribute.name + "__size"] = value
            elif filtername == "exists":
                d[attribute.name + "__exists"] = value
            elif filtername == "iexact":
                d[attribute.name + "__iexact"] = value
            elif filtername == "contains":
                d[attribute.name + "__contains"] = value
            elif filtername == "icontains":
                d[attribute.name + "__icontains"] = value
            elif filtername == "startswith":
                d[attribute.name + "__startswith"] = value
            elif filtername == "istartswith":
                d[attribute.name + "__istartswith"] = value
            elif filtername == "endswith":
                d[attribute.name + "__endswith"] = value
            elif filtername == "iendswith":
                d[attribute.name + "__iendswith"] = value
            elif filtername == "match":
                d[attribute.name + "__match"] = value
            else:
                raise jsonapi.base.errors.UnfilterableField(filtername,
                                                            fieldname)
        return d

    def _build_order_criterion(self, schema_, order):
        """
        TODO: for now works as simple request, add order

        :arg jsonapi.py2neo.schema.Schema schema_:
        :arg order:
        """
        criterion = list()
        for direction, fieldname in order:

            # We only support sorting for attributes at the moment.
            attribute = schema_.attributes.get(fieldname)
            if not isinstance(attribute, schema.Attribute):
                raise jsonapi.base.errors.UnsortableField(schema_.typename,
                                                          fieldname)

            criterion.append(direction + attribute.name)
        return criterion

    def _build_query(self, typename,
                     *, order=None, limit=None, offset=None, filters=None
                     ):
        """
        """
        resource_class = self.api.get_resource_class(typename)
        schema_ = self.api.get_schema(typename)
        # print('_build_query resource class:', resource_class)
        # print('_build_query schema:', schema_)
        # print('_build_query py2neosession', self.py2neo_session,
        #       type(self.py2neo_session))

        if filters:
            filters = self._build_filter_criterion(schema_, filters)
            query = resource_class.select(self.py2neo_session).where(**filters)
        else:
            query = resource_class.select(self.py2neo_session)

        # if order:
        #     order = self._build_order_criterion(schema_, order)
        #     query = query.order_by(*order)

        # if offset:
        #     query = query.skip(offset)

        # if limit:
        #     query = query.limit(limit)
        return query

    def query(self, typename,
              *, order=None, limit=None, offset=None, filters=None
              ):
        """
        """
        query = self._build_query(
            typename, order=order, limit=limit, offset=offset, filters=filters
        )
        resources = list(query)
        print("If deleted:", resources)
        return resources

    def get(self, identifier, required=False):
        """
        """
        typename, resource_id = identifier
        resource_class = self.api.get_resource_class(typename)
        resource = resource_class.select(self.py2neo_session,
                                         int(resource_id)).first()

        if resource is None:
            raise jsonapi.base.errors.ResourceNotFound(identifier)
        return resource

    def get_many(self, identifiers, required=False):
        """
        Returns a dict, which maps each identifier in *identifiers* or None
        """
        # print("Identifiers in get_many:", identifiers)
        # resources = {
        #     identifier: self.get(identifier, required)
        #     for identifier in identifiers
        # }
        # print("Resources in get_many:", resources)
        # return resources

    def save(self, resources):
        """
        """
        for resource in resources:
            self.py2neo_session.push(resource)

    def commit(self):
        """
        """
        pass

    def delete(self, resources):
        """
        """
        for resource in resources:
            self.py2neo_session.delete(resource)

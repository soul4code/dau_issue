from django.db.models import Index


class CustomIndex(Index):
    def __init__(self, expression: str, name):
        self.expression = expression
        self.fields = [expression]
        self.name = name

    def get_sql_create_template_values(self, model, schema_editor, using):
        quote_name = schema_editor.quote_name

        return {
            'table': quote_name(model._meta.db_table),
            'name': quote_name(self.name),
            'columns': self.expression,
            'using': using,
            'extra': '',
        }

    def create_sql(self, model, schema_editor, using=''):
        sql_create_index = schema_editor.sql_create_index
        sql_parameters = self.get_sql_create_template_values(model, schema_editor, using)
        return sql_create_index % sql_parameters

    def remove_sql(self, model, schema_editor):
        quote_name = schema_editor.quote_name
        return schema_editor.sql_delete_index % {
            'table': quote_name(model._meta.db_table),
            'name': quote_name(self.name),
        }

    def deconstruct(self):
        path = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        path = path.replace('django.db.models.indexes', 'django.db.models')
        return (path, (), {'expression': self.expression, 'name': self.name})

    def clone(self):
        """Create a copy of this Index."""
        path, args, kwargs = self.deconstruct()
        return self.__class__(*args, **kwargs)

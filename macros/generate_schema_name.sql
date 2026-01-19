{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schemaname is none -%}

       {%- if target.schema != "prod" -%}
            {% if node.fqn[1:-1]|length == 0 %}
                 {{target.schema}}{{ defaultschema }}
            {% else %}
                {% set prefix = node.fqn[1:-1]|join('') %}
                 {{target.schema}}_{{ prefix | trim }}
            {% endif %}


       {% else %} 
            {% if node.fqn[1:-1]|length == 0 %}
                {{ defaultschema }}
            {% else %}
                {% set prefix = node.fqn[1:-1]|join('') %}
                {{ prefix | trim }}
            {% endif %}
         {% endif %}
    {%- else -%}

        {{ defaultschema }}{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}

{% macro to_bool(val, default=true) %}
  {% if val is string %}
    {% set v = val|lower %}
    {{ (v in ['1','true','t','yes','y']) }}
  {% else %}
    {{ default }}
  {% endif %}
{% endmacro %}
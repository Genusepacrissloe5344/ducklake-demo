{% materialization table, adapter="duckdb", supported_languages=['sql', 'python'] %}
  {%- set language = model['language'] -%}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') %}
  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- For DuckLake: drop and recreate with the final name directly,
  -- avoiding the __dbt_tmp suffix that gets baked into S3 paths.
  {% if existing_relation is not none %}
      {% do drop_indexes_on_relation(existing_relation) %}
      {{ drop_relation_if_exists(existing_relation) }}
  {% endif %}

  {% call statement('main', language=language) -%}
    {{- create_table_as(False, target_relation, compiled_code, language) }}
  {%- endcall %}

  {% do create_indexes(target_relation) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}

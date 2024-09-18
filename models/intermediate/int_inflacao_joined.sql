-- models/intermediate/int_inflacao_joined.sql

with stg_bacen__igpm_mes as (
    select * from {{ ref('stg_bacen__igpm_mes') }}
),

-- transformação dos dados
int_inflacao_joined as (
    select  
    igpm.Data,
    igpm.IGPM_Mes

    from stg_bacen__igpm_mes igpm
)

-- retorno dos dados
select * from int_inflacao_joined where Data >= '2012-01-01'
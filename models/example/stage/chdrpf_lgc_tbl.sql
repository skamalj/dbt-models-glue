{{ config(materialized='view') }}
select distinct chdrnum
,chdrcoy
,recode
,servunit
,cnttype
,tranno
,validflag
,currfrom
,currto
,proctrancd
,procflag
,procid
,statcode
,statreasn
,statdate
,stattran
,tranlused
,occdate from {{ source('flights','chdrpf') }}

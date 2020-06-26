# -*- coding: utf-8 -*-
"""
Created on Wed Jun 10 12:05:10 2020

@author: Marco
"""

from Commons import one_house as OH

def gen_data(fecha_ini, fecha_fin):
    OH.exec_string(""" drop table if exists MZ_TABLON_OPTOUT""")
    OH.exec_string(""" drop table if exists MZ_TABLON_OPTOUTV2""")
    OH.exec_string(""" 
                         create table MZ_TABLON_OPTOUT as
            select email_cliente, flag_unsubs2 as flag_unsubs, job_envio,fecha_ult_envio, fecha_desus,
            cantidad_ult3, cantidad_ult7, cantidad_ult1,
            case when cantidad_ult15-cantidad_ult7 = 0 then 0 else cantidad_ult7/(cantidad_ult15-cantidad_ult7) end as var_cant_15_7,
            case when cantidad_30_60  = 0 then 0 else cantidad_ult30/cantidad_30_60 end as var_cant_60_30, 
            
            case when ((open_ult15-open_ult7)/(cantidad_ult15-cantidad_ult7)) = 0 or 
            cantidad_ult15-cantidad_ult7 = 0 or (open_ult15-open_ult7) = 0 or cantidad_ult7 = 0 then 0 else
            (open_ult7/cantidad_ult7)/((open_ult15-open_ult7)/(cantidad_ult15-cantidad_ult7)) end 
            as var_open_15_7,
            
            case when open_30_60/cantidad_30_60 = 0 or cantidad_30_60 = 0 or cantidad_ult30 = 0
            then 0 else (open_ult30/cantidad_ult30)/(open_30_60/cantidad_30_60) end as var_open_15
            
            from (
            select a.email_cliente,
               case when fecha_desus is not null then '1' else '0' end flag_unsubs2,
               c.fecha_ult_envio,
               b.fecha_desus,
               c.job_envio,
               
               count(case when a.fecha_real >= adddate(nvl(fecha_desus, fecha_ult_envio),-1) and 
               a.fecha_real < nvl(fecha_desus, fecha_ult_envio) 
               then a.email_cliente else null end) as cantidad_ult1,
               
               count(case when a.fecha_real >= adddate(nvl(fecha_desus, fecha_ult_envio),-7) and 
               a.fecha_real < nvl(fecha_desus, fecha_ult_envio) 
               then a.email_cliente else null end) as cantidad_ult7,
               
               count(case when a.fecha_real >= adddate(nvl(fecha_desus, fecha_ult_envio),-3) and 
               a.fecha_real < nvl(fecha_desus, fecha_ult_envio) 
               then a.email_cliente else null end) as cantidad_ult3,
               
               count(case when a.fecha_real >= adddate(nvl(fecha_desus, fecha_ult_envio),-15) and 
               a.fecha_real < nvl(fecha_desus, fecha_ult_envio) 
               then a.email_cliente else null end) as cantidad_ult15,
               
               count(case when a.fecha_real >= adddate(nvl(fecha_desus, fecha_ult_envio),-30) and 
               a.fecha_real < nvl(fecha_desus, fecha_ult_envio) 
               then a.email_cliente else null end) as cantidad_ult30,
               
               count(case when a.fecha_real >= adddate(nvl(fecha_desus, fecha_ult_envio),-60) and 
               a.fecha_real < adddate(nvl(fecha_desus, fecha_ult_envio),-30)
               then a.email_cliente else null end) as cantidad_30_60,
               
               sum(case when a.fecha_real >= adddate(nvl(fecha_desus, fecha_ult_envio),-15) and 
               a.fecha_real < nvl(fecha_desus, fecha_ult_envio) 
               then cast(a.flag_open as int) else 0 end) as open_ult15,
               
               sum(case when a.fecha_real >= adddate(nvl(fecha_desus, fecha_ult_envio),-7) and 
               a.fecha_real < nvl(fecha_desus, fecha_ult_envio) 
               then cast(a.flag_open as int) else 0 end) as open_ult7,
               
               sum(case when a.fecha_real >= adddate(nvl(fecha_desus, fecha_ult_envio),-30) and 
               a.fecha_real < nvl(fecha_desus, fecha_ult_envio) 
               then cast(a.flag_open as int) else 0 end) as open_ult30, 
               
               sum(case when a.fecha_real >= adddate(nvl(fecha_desus, fecha_ult_envio),-60) and 
               a.fecha_real < adddate(nvl(fecha_desus, fecha_ult_envio),-30)
               then cast(a.flag_open as int) else 0 end) as open_30_60
               
        from sod_onehouse_exp.vw_emm_cm_socl_mail A
        LEFT JOIN (
                select email_cliente, max(fecha_real) as fecha_desus
                from sod_onehouse_exp.vw_emm_cm_socl_mail 
                where flag_unsubs = '1' and  cast(fecha_real as timestamp) >='""" + fecha_ini + """' and
        cast(fecha_real as timestamp) <'""" + fecha_fin + """' group by email_cliente ) B on a.email_cliente = b.email_cliente
        
        LEFT JOIN (
                select email_cliente, max(fecha_real) as fecha_ult_envio, max(id_jobenvio) as job_envio
                from sod_onehouse_exp.vw_emm_cm_socl_mail 
                where cast(fecha_real as timestamp) >= '""" + fecha_ini + """' and
        cast(fecha_real as timestamp) < '""" + fecha_fin + """' 
                group by email_cliente) C on a.email_cliente = c.email_cliente
        
        where  cast(a.fecha_real as timestamp) >= '""" + fecha_ini + """' and
        cast(a.fecha_real as timestamp) < '""" + fecha_fin +"""' 
        group by a.email_cliente, flag_unsubs2,c.fecha_ult_envio,
               b.fecha_desus,c.job_envio) A
                """)


    OH.exec_string(""" 
              create table MZ_TABLON_OPTOUTV2 as
                   select A.*, B.total_mails, 
                   case when dayofweek(fecha_ult_envio) = 3 or dayofweek(fecha_ult_envio) = 4 
                   then 1 else 0 end mar_mier, dayofweek(fecha_ult_envio) as dia_sem,
                   case when dayofweek(fecha_ult_envio) = 4 then 1 else 0 end mier,
                   case when cantidad_ult3 > 4 then 1 else 0 end may4_ult3,
                   case when cantidad_ult7 > 7 then 1 else 0 end may7_ult7,
                   case when total_mails > 450000 then 1 else 0 end tot_mails_may450,
                   case when var_cant_15_7 > 3 then 1 else 0 end var_cant_15_7_may3
                     from MZ_TABLON_OPTOUT A left join (
                     select distinct id_jobenvio, total_mails from
                     sod_onehouse_exp.vw_emm_cm_socl_jobenvio
                     ) B  on A.job_envio = B.id_jobenvio                                       
                             """)
    OH.exec_string(""" drop table MZ_TABLON_OPTOUT""")
    tablon = OH.query_string(""" select * from MZ_TABLON_OPTOUTV2""")
    return tablon
    OH.exec_string(""" drop table MZ_TABLON_OPTOUTV2""")
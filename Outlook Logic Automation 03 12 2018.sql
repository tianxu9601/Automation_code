----------------- insert into table sh_fsc_base

sel * from dw_cal_dt where cal_dt>'2018-03-03' and cal_dt < '2019-06-02';

insert into p_ci_map_t.sh_fsc_base values ('2018-03-04' , 'Sunday', '2018-03-04' , '2018-03-10' , 10.00, 10.00, 13.00, 3.00, 1.00, 2018.00, '2018W10'  , '2018W10' , '2018M03', '2018Q01', '2018M03', '2018Q01' );
insert into p_ci_map_t.sh_fsc_base values ('2018-03-05' , 'Monday', '2018-03-04' , '2018-03-10' , 10.00, 10.00, 13.00, 3.00, 1.00, 2018.00, '2018W10' , '2018W10' , '2018M03', '2018Q01', '2018M03', '2018Q01' );
insert into p_ci_map_t.sh_fsc_base values ('2018-03-06' , 'Tuesday', '2018-03-04' , '2018-03-10' , 10.00, 10.00, 13.00, 3.00, 1.00, 2018.00, '2018W10'  , '2018W10' , '2018M03', '2018Q01', '2018M03', '2018Q01' );
insert into p_ci_map_t.sh_fsc_base values ('2018-03-07' , 'Wednesday', '2018-03-04' , '2018-03-10' , 10.00, 10.00, 13.00, 3.00, 1.00, 2018.00, '2018W10'  , '2018W10' , '2018M03', '2018Q01', '2018M03', '2018Q01' );
insert into p_ci_map_t.sh_fsc_base values ('2018-03-08' , 'Thursday', '2018-03-04' , '2018-03-10' , 10.00, 10.00, 13.00, 3.00, 1.00, 2018.00, '2018W10'  , '2018W10' , '2018M03', '2018Q01', '2018M03', '2018Q01' );
insert into p_ci_map_t.sh_fsc_base values ('2018-03-09' , 'Friday', '2018-03-04' , '2018-03-10' , 10.00, 10.00, 13.00, 3.00, 1.00, 2018.00, '2018W10'  , '2018W10' , '2018M03', '2018Q01', '2018M03', '2018Q01' );
insert into p_ci_map_t.sh_fsc_base values ('2018-03-10' , 'Saturday', '2018-03-04' , '2018-03-10' , 10.00, 10.00, 13.00, 3.00, 1.00, 2018.00, '2018W10'  , '2018W10' , '2018M03', '2018Q01', '2018M03', '2018Q01' );

show table  p_ci_map_t.sh_fsc_base;




------------- Earnings
Drop	table p_ci_map_t.jsh_fcst_1 ;
Create	multiset table p_ci_map_t.jsh_fcst_1 as(
select	
MONTH_END_DT,
RETAIL_WK_END_DATE,
QTR_END_DT,
      TRANS_DT as CK_TRANS_DT,
      AMS_PRGRM_ID,
          case
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
		-999) = 1 then 'OCS'
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
		-999) in (2,3) then 'Content'
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
		-999) = 4 then 'Loyalty'
            else 'Other' 
end	as BM,
      --sum(ERNG_USD) as ERNG_USD,
      sum(ERNG_PRGRM_CRNCY) as ERNG_PRG
    from
      prs_ams_v.AMS_PBLSHR_ERNG a
      join dw_cal_dt cal
      on a.trans_dt = cal.cal_dt
      join prs_ams_v.AMS_PBLSHR c
      on a.AMS_PBLSHR_ID = c.AMS_PBLSHR_ID 

      where 
      TRANS_DT between '2015-12-26' and '2018-03-10'
      and AMS_PRGRM_ID in (2,3,5,10,11,12,13,14,15,16,4,1,7)
      and a.ams_pblshr_id not in (5575245877,5575245881,5575245884,
		5575245888,5575246818,5575245887) -- excluding eCG pubs, as they don't earn
    group by 1,2,3,4,5,6)
    with data primary index(CK_TRANS_DT,AMS_PRGRM_ID,BM);
	
	
	
	------- Rev
	
	drop table p_ci_map_t.jsh_fcst_2;
   Create multiset table p_ci_map_t.jsh_fcst_2 as(
select      
MONTH_END_DT,
RETAIL_WK_END_DATE,
QTR_END_DT,
      FAM2.CK_TRANS_DT,
      P.AMS_PRGRM_ID,
      case
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
                                -999) = 1 then 'OCS'
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
                                -999) in (2,3) then 'Content'
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
                                -999) = 4 then 'Loyalty'
            else 'Other' 
end         as BM,
      sum(FAM2.IREV_PLAN_RATE_AMT) as REV_USD       
      --sum(FAM2.IGMB_PLAN_AMT) as iGMB_USD,
      --sum(FAM2.IREV_PLAN_AMT) as iREV_USD
    from
      PRS_RESTRICTED_V.MH_IM_CORE_FAM2_FACT  as FAM2
      join prs_ams_v.AMS_PRGRM P
        on FAM2.CLIENT_ID=P.MPX_CLNT_ID
      join dw_cal_dt cal
      on FAM2.CK_TRANS_DT = cal.cal_dt  
        join prs_ams_v.AMS_PBLSHR C
            on FAM2.epn_PBLSHR_ID = C.AMS_PBLSHR_ID--------------
    
    where CK_TRANS_DT  between '2017-01-01' and '2018-03-10'

      and P.AMS_PRGRM_ID in (2,3,5,10,11,12,13,14,15,16,4,1,7)
      and FAM2.CLIENT_ID in (707, 709, 710, 724, 1185, 1346, 5282,                     5221, 1553, 5222, 705, 711, 706)
      and FAM2.MPX_CHNL_ID=6-------------------------------
      and C.ams_pblshr_id not in (5575245877,5575245881,5575245884,                             5575245888,5575246818,5575245887) -- excluding eCG pubs, as they don't earn
    group by 1,2,3,4,5,6)
    with data primary index(CK_TRANS_DT,AMS_PRGRM_ID,BM);
	
	
	
	------- iGMB Desktop

Drop	table p_ci_map_t.jsh_fcst_3;    
Create	multiset table p_ci_map_t.jsh_fcst_3 as(
    select
MONTH_END_DT,
RETAIL_WK_END_DATE,
QTR_END_DT,
      FAM2.CK_TRANS_DT,
      P.AMS_PRGRM_ID,
       case
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
		-999) = 1 then 'OCS'
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
		-999) in (2,3) then 'Content'
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
		-999) = 4 then 'Loyalty'
            else 'Other' 
end	as BM,
      sum(FAM2.IGMB_PLAN_RATE_AMT) as iGMB_USD_Desktop,-------------------------
      sum(FAM2.IREV_PLAN_RATE_AMT) as iREV_USD_Desktop
    from
      PRS_RESTRICTED_V.MH_IM_CORE_FAM2_FACT  as FAM2
      join prs_ams_v.AMS_PRGRM P
        on FAM2.CLIENT_ID=P.MPX_CLNT_ID
          join prs_ams_v.AMS_PBLSHR C
            on FAM2.EPN_PBLSHR_ID = C.AMS_PBLSHR_ID------------
     join dw_cal_dt cal
      on  FAM2.CK_TRANS_DT = cal.cal_dt  
    where CK_TRANS_DT  between '2017-01-01' and '2018-03-10'
      --where FAM2.CK_TRANS_DT between date - 120 and date --date '2013-01-01' and date '2017-02-04'
      and P.AMS_PRGRM_ID in (2,3,5,10,11,12,13,14,15,16,4,1,7)
      and FAM2.CLIENT_ID in (707, 709, 710, 724, 1185, 1346, 5282,
		5221, 1553, 5222, 705, 711, 706)
      and FAM2.MPX_CHNL_ID=6-------------
      and FAM2.DEVICE_TYPE_ID=1
	    and FAM2.SAP_CATEGORY_ID NOT IN (5, 7, 41, 23, -999)
      and C.ams_pblshr_id not in (5575245877,5575245881,5575245884,
		5575245888,5575246818,5575245887) -- excluding eCG pubs, as they don't earn
    group by 1,2,3,4,5,6)
    with data primary index(CK_TRANS_DT,AMS_PRGRM_ID,BM);
	
	
	
	Drop	table p_ci_map_t.jsh_fcst_4;  
Create	multiset table p_ci_map_t.jsh_fcst_4 as(
    select
MONTH_END_DT,
RETAIL_WK_END_DATE,
QTR_END_DT,
      A.CK_TRANS_DT,
      B.AMS_PRGRM_ID,
       case
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
		-999) = 1 then 'OCS'
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
		-999) in (2,3) then 'Content'
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
		-999) = 4 then 'Loyalty'
            else 'Other' 
end	as BM,
      sum(A.IGMB_PLAN_RATE_AMT) as iGMB_USD_Mobile,
      sum(A.IREV_PLAN_RATE_AMT) as iREV_USD_Mobile
    from
      PRS_RESTRICTED_V.MH_IM_CORE_FAM2_FACT  as A
      join prs_ams_v.AMS_PRGRM B
        on A.CLIENT_ID=B.MPX_CLNT_ID---------------------------
      join dw_cal_dt cal
      on a.CK_TRANS_DT = cal.cal_dt  
      join prs_ams_v.AMS_PBLSHR C
            on A.EPN_PBLSHR_ID = C.AMS_PBLSHR_ID
    
     where CK_TRANS_DT  between '2017-01-01' and '2018-03-10'    
     -- where  A.CK_TRANS_DT between date - 120 and date --date '2013-01-01' and date '2017-02-04'
      and B.AMS_PRGRM_ID in (2,3,5,10,11,12,13,14,15,16,4,1,7)
      and A.CLIENT_ID in (707, 709, 710, 724, 1185, 1346, 5282,
		5221, 1553, 5222, 705, 711, 706)
      and A.MPX_CHNL_ID = 6   ---------------------------------------
      and A.DEVICE_TYPE_ID = 2--------------------  
      and A.SAP_CATEGORY_ID NOT IN (5, 7, 41, 23, -999)       ---------------------------------------------------------
      and C.ams_pblshr_id not in (5575245877,5575245881,5575245884,
		5575245888,5575246818,5575245887) -- excluding eCG pubs, as they don't earn
    group by 1,2,3,4,5,6)
    with data primary index(CK_TRANS_DT,AMS_PRGRM_ID,BM);
	
	
	
	Drop	table p_ci_map_t.jsh_fcst_5;
Create	multiset table p_ci_map_t.jsh_fcst_5 as
(
select	
MONTH_END_DT,
RETAIL_WK_END_DATE,
QTR_END_DT,
CK_TRANS_DT,
prg_lkp as program,
BM,
sum(ERNG_PRG) as ERNG_PRG,
--sum(ERNG_USD) as ERNG_USD,
sum(REV_USD) as REV_USD,
sum(iGMB_USD) as iGMB_USD,
sum(coalesce(iRev_USD,0)) as iRev_USD,

sum(iGMB_USD_Desktop) as iGMB_USD_Desktop,
sum(iGMB_USD_Mobile) as iGMB_USD_Mobile,
sum(iRev_USD_Desktop) as iRev_USD_Desktop,
sum(iREV_USD_Mobile) as iREV_USD_Mobile

from	

(
Select	
a.*, 
Case	
	when	a.AMS_PRGRM_ID = 1 then 'US'
	when	a.ams_prgrm_id = 2 then 'ROE'
	when	a.ams_prgrm_id = 3 then 'ROE'
	when	a.ams_prgrm_id = 4 then 'AU'
	when	a.ams_prgrm_id = 5 then 'ROE'
	when	a.ams_prgrm_id = 7 then 'CA'
	when	a.ams_prgrm_id = 10 then 'FR'
	when	a.ams_prgrm_id = 11 then 'DE'
	when	a.ams_prgrm_id = 12 then 'IT'
	when	a.ams_prgrm_id = 13 then 'ES'
	when	a.ams_prgrm_id = 14 then 'ROE'
	when	a.ams_prgrm_id = 15 then 'UK'
	when	a.ams_prgrm_id = 16 then 'ROE'
	when	a.ams_prgrm_id = 17 then 'Half'
else	'Others'
end	as prg_lkp,
b.REV_USD, 
C.iGMB_USD_Desktop,
coalesce(d.iGMB_USD_Mobile,0) as iGMB_USD_Mobile,
C.iRev_USD_Desktop,
coalesce(D.iREV_USD_Mobile,0) as iREV_USD_Mobile,
C.iGMB_USD_Desktop+coalesce(d.iGMB_USD_Mobile,0) as iGMB_USD, 
C.iRev_USD_Desktop +coalesce(D.iREV_USD_Mobile,0) as iRev_USD

from	p_ci_map_t.jsh_fcst_1 a
join p_ci_map_t.jsh_fcst_2 b
	on	a.CK_TRANS_DT = b.CK_TRANS_DT
	and	a.AMS_PRGRM_ID = b.AMS_PRGRM_ID
	and	a.BM = b.BM
join p_ci_map_t.jsh_fcst_3 c
	on	a.CK_TRANS_DT = c.CK_TRANS_DT
	and	a.AMS_PRGRM_ID = c.AMS_PRGRM_ID
	and	a.BM = c.BM
left join p_ci_map_t.jsh_fcst_4 d
	on	a.CK_TRANS_DT = d.CK_TRANS_DT
	and	a.AMS_PRGRM_ID = d.AMS_PRGRM_ID
	and	a.BM = d.BM)a
group	by 1,2,3,4,5,6
)
with	data primary index(CK_TRANS_DT,program,BM);






Drop	table p_ci_map_t.jsh_fcst_6;
Create	multiset table p_ci_map_t.jsh_fcst_6 as
(
select	
a.*,
WEEK_OF_YEAR_ID,
YEAR_ID,
case	
	when	program = 'AU' then (ERNG_PRG*1.0000/0.76) --not updated
	when	program = 'UK' then (ERNG_PRG*1.0000/1.33) 
	when	program = 'US' then (ERNG_PRG*1.0000/1) 
	when	program = 'CA' then (ERNG_PRG*1.0000/1) 
else	 ERNG_PRG
end	as ERNG_PRG_lc,
case	
	when	program = 'AU' then (ERNG_PRG * 0.76) -- AUD
	when	program = 'UK' then (ERNG_PRG * 1.33) -- Pound Sterling
	when	program = 'US' then (ERNG_PRG * 1) 
	when	program = 'CA' then (ERNG_PRG * 1) 
	when	program = 'DE' then (ERNG_PRG * 1.18) -- Euro
	when	program = 'FR' then (ERNG_PRG * 1.18) -- Euro
	when	program = 'IT' then (ERNG_PRG * 1.18) -- Euro
	when	program = 'ES' then (ERNG_PRG * 1.18) -- Euro
	when	program = 'ROE' then (ERNG_PRG * 1.18) -- Euro
else	 ERNG_PRG
end	as ERNG_USD,
case	
	when	program = 'AU' then (REV_USD*1.0000/0.76) --not updated
	when	program = 'UK' then (REV_USD*1.0000/1.33) 
	when	program = 'US' then (REV_USD*1.0000/1) 
	when	program = 'CA' then (REV_USD*1.0000/1) 
else	 REV_USD
end	as REV_USD_lc,
case	
	when	program = 'AU' then (iGMB_USD*1.0000/0.76) --not updated
	when	program = 'UK' then (iGMB_USD*1.0000/1.33) 
	when	program = 'US' then (iGMB_USD*1.0000/1) 
	when	program = 'CA' then (iGMB_USD*1.0000/1) 
else	 iGMB_USD
end	as iGMB_USD_lc,
case	
	when	program = 'AU' then (iRev_USD*1.0000/0.76) --not updated
	when	program = 'UK' then (iRev_USD*1.0000/1.33) 
	when	program = 'US' then (iRev_USD*1.0000/1) 
	when	program = 'CA' then (iRev_USD*1.0000/1) 
else	 iRev_USD
end	as iRev_USD_lc

from	p_ci_map_t.jsh_fcst_5 a
join dw_cal_dt cal
	on	a.CK_TRANS_DT = cal.cal_dt)
with	data primary index(CK_TRANS_DT,program,BM);




DROP TABLE Test_mult;
create	volatile table Test_mult as (
Select	
a.*,
b.*
From	
(
Select	
MONTH_END_DT,                   
RETAIL_WK_END_DATE,             
QTR_END_DT,                     
program ,                       
BM  ,                           
--sum(ERNG_PRG_lc) as ERNG_PRG,
sum(ERNG_USD) as ERNG_USD,
sum(REV_USD) as REV_USD,
sum(iGMB_USD) as iGMB_USD,
sum(iRev_USD) as iRev_USD,
sum(iGMB_USD_Desktop) as iGMB_USD_Desktop,
sum(iGMB_USD_Mobile) as iGMB_USD_Mobile,
sum(iRev_USD_Desktop) as iRev_USD_Desktop,
sum(iREV_USD_Mobile) as iREV_USD_Mobile
--sum(WEEK_OF_YEAR_ID) as WEEK_OF_YEAR_ID,
--sum(YEAR_ID) as YEAR_ID
--sum(ERNG_PRG_lc) as ERNG_PRG_lc,
--sum(REV_USD_lc) as REV_USD_lc,
--sum(iGMB_USD_lc) as iGMB_USD_lc,
--sum(iRev_USD_lc) as iRev_USD_lc
from	p_ci_map_t.jsh_fcst_6
group	by 1,2,3,4,5) a
left join 
( 
Select	wk_end_dt,
fsc_wk,
fsc_mnth_num as fsc_mnth,
Case	
	when	fsc_mnth_num = 1 then 'Jan'
	when	fsc_mnth_num = 2 then 'Feb'
	when	fsc_mnth_num = 3 then 'Mar'
	when	fsc_mnth_num = 4 then 'Apr'
	when	fsc_mnth_num = 5 then 'May'
	when	fsc_mnth_num = 6 then 'Jun'
	when	fsc_mnth_num = 7 then 'Jul'
	when	fsc_mnth_num = 8 then 'Aug'
	when	fsc_mnth_num = 9 then 'Sep'
	when	fsc_mnth_num = 10 then 'Oct'
	when	fsc_mnth_num = 11 then 'Nov'
	when	fsc_mnth_num = 12 then 'Dec'
else	'Others'
end	as fsc_Mnth2,
fsc_qtr_num as Fsc_Qtr,
Fsc_Yr
from	
p_ci_map_t.sh_fsc_base
group	by 1,2,3,4,5,6) b
	on	a.RETAIL_WK_END_DATE = b.wk_end_dt
)
with	data primary index(MONTH_END_DT,RETAIL_WK_END_DATE,QTR_END_DT,
		program ,BM) 
	on	commit preserve rows;
	
	
	
	
	drop table test;
create	volatile table test as(
select	* 
from	p_ci_map_t.jsh_EPN_FCST_Output a 
where	RETAIL_WK_END_DATE < (
select	min(RETAIL_WK_END_DATE) 
from	p_ci_map_t.jsh_fcst_6)
)
with	data 
	on	commit preserve rows;
	
	
	
	
	Drop	table p_ci_map_t.jsh_EPN_FCST_Output;
create	multiset table p_ci_map_t.jsh_EPN_FCST_Output  as (
Select	a.* 
from	test a
union	
select	b.* 
from	Test_mult b

)
with	data primary index(MONTH_END_DT,RETAIL_WK_END_DATE,QTR_END_DT,
		program ,BM  );
		
		
		
		sel	*
from	p_ci_map_t.jsh_EPN_FCST_Output;
	
	
    
	
	
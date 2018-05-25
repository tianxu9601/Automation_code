Drop	table p_ci_map_t.jsh_EPN_FCST_iAB_Output;
create	multiset table p_ci_map_t.jsh_EPN_FCST_iAB_Output  as (
sel           
dt.fsc_wk,
dt.fsc_mnth_num as fsc_mnth,
Case	
	when	dt.fsc_mnth_num = 1 then 'Jan'
	when	dt.fsc_mnth_num = 2 then 'Feb'
	when	dt.fsc_mnth_num = 3 then 'Mar'
	when	dt.fsc_mnth_num = 4 then 'Apr'
	when	dt.fsc_mnth_num = 5 then 'May'
	when	dt.fsc_mnth_num = 6 then 'Jun'
	when	dt.fsc_mnth_num = 7 then 'Jul'
	when	dt.fsc_mnth_num = 8 then 'Aug'
	when	dt.fsc_mnth_num = 9 then 'Sep'
	when	dt.fsc_mnth_num = 10 then 'Oct'
	when	dt.fsc_mnth_num = 11 then 'Nov'
	when	dt.fsc_mnth_num = 12 then 'Dec'
else	'Others'
end	as fsc_Mnth2,
dt.fsc_qtr_num as Fsc_Qtr,
dt.Fsc_Yr,
                   ------- dt.WEEK_OF_YEAR_ID, dt.retail_year,                                                                                               
                             CASE                    
                                           WHEN B.AMS_PRGRM_ID IN (1) THEN 'US'                            
                                           WHEN B.AMS_PRGRM_ID IN (7) THEN 'CA'           
                                           WHEN B.AMS_PRGRM_ID = 4 THEN 'AU'                              
                                           WHEN B.AMS_PRGRM_ID = 11 THEN 'DE'                              
                                           WHEN B.AMS_PRGRM_ID = 15 THEN 'UK'                              
                                           WHEN B.AMS_PRGRM_ID =10 THEN 'FR'                              
                                           WHEN B.AMS_PRGRM_ID=12 THEN 'IT'                
                                           WHEN B.AMS_PRGRM_ID=13 THEN 'ES'                              
                                           WHEN B.AMS_PRGRM_ID IN (2,3,5,14,16) THEN 'ROE'                              
                                           ELSE 'OTHERS'                  
                             END AS REGION,                            
                             case                     
                                                          when CLV_BUYER_TYPE_CD in (1,2) then 'Acquired'
                                                          when CLV_BUYER_TYPE_CD in (101) then 'Engaged'
                                                          when CLV_BUYER_TYPE_CD in (102) then 'Retained'
                                                          else 'Existing'
                             end as txn_type,
                             CASE
    WHEN region='US' THEN 0.65
        WHEN region='CA' THEN 0.69
    WHEN region='UK' THEN 0.54
    WHEN region='DE' THEN 0.55
    WHEN region='FR' THEN 0.73
    WHEN region='IT' THEN 0.65
    WHEN region='ES' THEN 0.70
    WHEN region='ROE' THEN 0.52

WHEN region='AU' THEN 0.52
    ELSE 0
  END AS RC_FCTR,
                             CLV_BUYER_TYPE_CD,                 
                             SUM(CAST(GMB_USD_AMT AS DECIMAL(24,6))) AS GMB_USD,                             
                             SUM(CAST(IGMB_USD_AMT  AS DECIMAL(24,6))) AS IGMB_USD,                      
                             sum(cast(IGMB_PLAN_RATE_AMT as DECIMAL(24,6))) AS iGMB_Plan,                      
                             SUM(CAST(DGMB_USD_AMT  AS DECIMAL(24,6))) AS DGMB_USD,                    
                             SUM(CAST(IGMB_USD_AMT AS DECIMAL(24,6)) - CAST(DGMB_USD_AMT AS DECIMAL(24,6))) AS ICAV_USD_Calc,               
                             sum(Cast( iCAV_USD_AMT as DEcimal (24,6))) as iCAV_USD_AMT,                           
                             sum(Cast( CAV_USD_AMT as DEcimal (24,6))) as CAV_USD_AMT,                            
                             SUM(CAST(IREV_USD_AMT  AS DECIMAL(24,6))) AS IREV_USD,                             
                             sum(cast(IREV_PLAN_RATE_AMT as DECIMAL(24,6))) AS iREV_Plan,         
                             SUM(CASE WHEN CLV_BUYER_TYPE_CD IN (1,2) THEN 1 ELSE 0 END) AS NORB_COUNT,               
                             SUM(CASE WHEN CLV_BUYER_TYPE_CD IN (1,2) THEN INCR_FCTR ELSE 0 END) AS INORB_COUNT, SUM(CASE WHEN CLV_BUYER_TYPE_CD IN (101) THEN 1 ELSE 0 END) AS CLVE_cnt, SUM(CASE WHEN CLV_BUYER_TYPE_CD IN (102) THEN 1 ELSE 0 END) AS CLVR_cnt,
                             SUM((CASE WHEN CLV_BUYER_TYPE_CD IN (102) THEN INCR_FCTR ELSE 0 END ) * RC_FCTR) AS IABR
FROM PRS_RESTRICTED_V.MH_IM_CORE_FAM2_FACT    FAM                                                
                             INNER JOIN p_ci_map_t.sh_fsc_base  DT ON FAM.CK_TRANS_DT = DT.CAL_DT            ----DW_CAL_DT DT           
                             JOIN PRS_AMS_V.AMS_PRGRM  B                          
                             ON FAM.CLIENT_ID=B.MPX_CLNT_ID                    
                             LEFT JOIN PRS_AMS_V.AMS_PBLSHR PBLSHR ON FAM.EPN_PBLSHR_ID = PBLSHR.AMS_PBLSHR_ID                           
                             LEFT JOIN PRS_AMS_V.AMS_PBLSHR_BSNS_MODEL PB_BM                              
                             ON PB_BM.PBLSHR_BSNS_MODEL_ID =COALESCE(PBLSHR.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, PBLSHR.PBLSHR_BSNS_MODEL_ID, -999)                            
WHERE                                            
             -- CK_TRANS_DT BETWEEN '2015-12-26' AND '2018-03-17'                
			  
			   CK_TRANS_DT > (
select	Min_Week_Dt 
from	Time_period)
      and CK_TRANS_DT <= (
select	Max_Week_Dt 
from	Time_period)
              AND MPX_CHNL_ID=6 --epn                                    
              AND FAM.CLIENT_ID IN (707, 709, 710, 724, 1185, 1346, 5282, 5221, 1553, 5222, 705, 711, 706)
              and epn_pblshr_id not in (5575245877,5575245881,5575245884,
                             5575245888,5575246818,5575245887) -- excluding eCG pubs, as they don't earn
GROUP BY 1,2,3,4,5,6,7,8,9)
with	data primary index (fsc_wk,  fsc_mnth, Fsc_Qtr, Fsc_Yr, region);

sel * from  p_ci_map_t.jsh_EPN_FCST_iAB_Output;

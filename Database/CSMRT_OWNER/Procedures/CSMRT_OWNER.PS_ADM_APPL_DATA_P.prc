CREATE OR REPLACE PROCEDURE             "PS_ADM_APPL_DATA_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_ADM_APPL_DATA'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_ADM_APPL_DATA', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_ADM_APPL_DATA'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ADM_APPL_DATA from PeopleSoft table PS_ADM_APPL_DATA.
--
-- V03  CASE-46298 08/03/2020,  Jim Doucette
--                              Fixed issue merging null ADM_CREATION_DT  
-- V02  CASE-46298 08/03/2020,  Jim Doucette
--                              Fixed issue merging null ADM_APPL_DT
-- V01  SMT-xxxx 06/05/2017,    Jim Doucette
--                              Converted from PS_ADM_APPL_DATA.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ADM_APPL_DATA';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;

BEGIN
strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strSqlCommand   := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_ADM_APPL_DATA'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ADM_APPL_DATA@SASOURCE S)
 where TABLE_NAME = 'PS_ADM_APPL_DATA'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ADM_APPL_DATA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ADM_APPL_DATA';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ADM_APPL_DATA T
using (select /*+ full(S) */
    nvl(trim(S.EMPLID),'-') EMPLID, 
    nvl(trim(S.ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(S.STDNT_CAR_NBR,0) STDNT_CAR_NBR, 
    nvl(trim(S.ADM_APPL_NBR),'-') ADM_APPL_NBR, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(ADM_APPL_CTR),'-') ADM_APPL_CTR, 
    nvl(trim(ADMIT_TYPE),'-') ADMIT_TYPE, 
    nvl(trim(FIN_AID_INTEREST),'-') FIN_AID_INTEREST, 
    nvl(trim(HOUSING_INTEREST),'-') HOUSING_INTEREST, 
    nvl(trim(APPL_FEE_STATUS),'-') APPL_FEE_STATUS, 
    to_date(to_char(case when APPL_FEE_DT < '01-JAN-1800' then NULL 
                    else APPL_FEE_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') APPL_FEE_DT, 
    nvl(trim(NOTIFICATION_PLAN),'-') NOTIFICATION_PLAN, 
    nvl(trim(REGION),'-') REGION, 
    nvl(trim(REGION_FROM),'-') REGION_FROM, 
    nvl(trim(RECRUITER_ID),'-') RECRUITER_ID, 
    nvl(trim(LAST_SCH_ATTEND),'-') LAST_SCH_ATTEND, 
    to_date(to_char(case when ADM_CREATION_DT < '01-JAN-1800' then NULL 
                    else ADM_CREATION_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') ADM_CREATION_DT, 
    nvl(trim(ADM_CREATION_BY),'-') ADM_CREATION_BY, 
    to_date(to_char(case when ADM_UPDATED_DT < '01-JAN-1800' then NULL 
                    else ADM_UPDATED_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') ADM_UPDATED_DT,
    nvl(trim(ADM_UPDATED_BY),'-') ADM_UPDATED_BY, 
    nvl(trim(ADM_APPL_COMPLETE),'-') ADM_APPL_COMPLETE, 
    to_date(to_char(case when ADM_APPL_DT < '01-JAN-1800' then NULL 
                    else ADM_APPL_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') ADM_APPL_DT, 
    to_date(to_char(case when ADM_APPL_CMPLT_DT < '01-JAN-1800' then NULL 
                    else ADM_APPL_CMPLT_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') ADM_APPL_CMPLT_DT, 
    to_date(to_char(case when GRADUATION_DT < '01-JAN-1800' then NULL 
               else GRADUATION_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') GRADUATION_DT, 
    nvl(trim(PRIOR_APPL),'-') PRIOR_APPL, 
    nvl(trim(APPL_FEE_TYPE),'-') APPL_FEE_TYPE, 
    nvl(trim(ADM_APPL_METHOD),'-') ADM_APPL_METHOD, 
    nvl(APPL_FEE_AMT,0) APPL_FEE_AMT, 
    nvl(APPL_FEE_PAID,0) APPL_FEE_PAID, 
    nvl(trim(CURRENCY_CD),'-') CURRENCY_CD, 
    nvl(trim(TENDER_CATEGORY),'-') TENDER_CATEGORY, 
    nvl(trim(ACADEMIC_LEVEL),'-') ACADEMIC_LEVEL, 
    nvl(trim(OVERRIDE_DEPOSIT),'-') OVERRIDE_DEPOSIT, 
    nvl(trim(EXT_ADM_APPL_NBR),'-') EXT_ADM_APPL_NBR, 
    nvl(trim(CREDIT_CARD_NBR),'-') CREDIT_CARD_NBR, 
    nvl(trim(CREDIT_CARD_TYPE),'-') CREDIT_CARD_TYPE, 
    nvl(trim(CREDIT_CARD_HOLDER),'-') CREDIT_CARD_HOLDER, 
    nvl(trim(CREDIT_CARD_ISSUER),'-') CREDIT_CARD_ISSUER, 
    to_date(to_char(case when CREDIT_CARD_EXP_DT < '01-JAN-1800' then NULL 
                    else CREDIT_CARD_EXP_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') CREDIT_CARD_EXP_DT,
    nvl(trim(CREDIT_CARD_STATUS),'-') CREDIT_CARD_STATUS, 
    nvl(trim(CREDIT_CARD_AUTHCD),'-') CREDIT_CARD_AUTHCD, 
    nvl(trim(CREDIT_CARD_DECLND),'-') CREDIT_CARD_DECLND, 
    nvl(trim(CREDIT_CARD_ERRMSG),'-') CREDIT_CARD_ERRMSG, 
    nvl(trim(CREDIT_CARD_VDAUTH),'-') CREDIT_CARD_VDAUTH, 
    nvl(trim(APP_FEE_STATUS),'-') APP_FEE_STATUS, 
    to_date(to_char(case when APP_FEE_CALC_DTTM < '01-JAN-1800' then NULL 
                    else APP_FEE_CALC_DTTM end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') APP_FEE_CALC_DTTM, 
    nvl(trim(CUR_RT_TYPE),'-') CUR_RT_TYPE, 
    nvl(RATE_MULT,0) RATE_MULT, 
    nvl(RATE_DIV,0) RATE_DIV, 
    nvl(ORIGNL_APPL_FEE_PD,0) ORIGNL_APPL_FEE_PD, 
    nvl(trim(ORIGNL_CURRENCY_CD),'-') ORIGNL_CURRENCY_CD, 
    nvl(WAIVE_AMT,0) WAIVE_AMT, 
    nvl(trim(SSF_IHC_PB),'-') SSF_IHC_PB,
    nvl(trim(R.UM_RA_TA_INTEREST),'-') UM_RA_TA_INTEREST
FROM SYSADM.PS_ADM_APPL_DATA@SASOURCE S
LEFT OUTER JOIN SYSADM.PS_UM_RA_TA@SASOURCE R
    ON S.EMPLID = R.EMPLID
   AND S.ACAD_CAREER = R.ACAD_CAREER
   AND S.STDNT_CAR_NBR = R.STDNT_CAR_NBR
   AND S.ADM_APPL_NBR = R.ADM_APPL_NBR
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APPL_DATA')
  and S.EMPLID between '00000000' and '99999999'
  and length(S.EMPLID) = 8 ) S
on ( 
    T.EMPLID = S.EMPLID and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.STDNT_CAR_NBR = S.STDNT_CAR_NBR and 
    T.ADM_APPL_NBR = S.ADM_APPL_NBR and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.INSTITUTION = S.INSTITUTION,
    T.ADM_APPL_CTR = S.ADM_APPL_CTR,
    T.ADMIT_TYPE = S.ADMIT_TYPE,
    T.FIN_AID_INTEREST = S.FIN_AID_INTEREST,
    T.HOUSING_INTEREST = S.HOUSING_INTEREST,
    T.APPL_FEE_STATUS = S.APPL_FEE_STATUS,
    T.APPL_FEE_DT = S.APPL_FEE_DT,
    T.NOTIFICATION_PLAN = S.NOTIFICATION_PLAN,
    T.REGION = S.REGION,
    T.REGION_FROM = S.REGION_FROM,
    T.RECRUITER_ID = S.RECRUITER_ID,
    T.LAST_SCH_ATTEND = S.LAST_SCH_ATTEND,
-- Case 46298, Aug 2020	
    --T.ADM_CREATION_DT = S.ADM_CREATION_DT,
	T.ADM_CREATION_DT = NVL(S.ADM_CREATION_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )),
-- Case 46298	
    T.ADM_CREATION_BY = S.ADM_CREATION_BY,
    T.ADM_UPDATED_DT = S.ADM_UPDATED_DT,
    T.ADM_UPDATED_BY = S.ADM_UPDATED_BY,
    T.ADM_APPL_COMPLETE = S.ADM_APPL_COMPLETE,
-- Case 46298, Aug 2020
--    T.ADM_APPL_DT = S.ADM_APPL_DT,
	T.ADM_APPL_DT = NVL(S.ADM_APPL_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )),
-- Case 46298
    T.ADM_APPL_CMPLT_DT = S.ADM_APPL_CMPLT_DT,
    T.GRADUATION_DT = S.GRADUATION_DT,
    T.PRIOR_APPL = S.PRIOR_APPL,
    T.APPL_FEE_TYPE = S.APPL_FEE_TYPE,
    T.ADM_APPL_METHOD = S.ADM_APPL_METHOD,
    T.APPL_FEE_AMT = S.APPL_FEE_AMT,
    T.APPL_FEE_PAID = S.APPL_FEE_PAID,
    T.CURRENCY_CD = S.CURRENCY_CD,
    T.TENDER_CATEGORY = S.TENDER_CATEGORY,
    T.ACADEMIC_LEVEL = S.ACADEMIC_LEVEL,
    T.OVERRIDE_DEPOSIT = S.OVERRIDE_DEPOSIT,
    T.EXT_ADM_APPL_NBR = S.EXT_ADM_APPL_NBR,
    T.CREDIT_CARD_NBR = S.CREDIT_CARD_NBR,
    T.CREDIT_CARD_TYPE = S.CREDIT_CARD_TYPE,
    T.CREDIT_CARD_HOLDER = S.CREDIT_CARD_HOLDER,
    T.CREDIT_CARD_ISSUER = S.CREDIT_CARD_ISSUER,
    T.CREDIT_CARD_EXP_DT = S.CREDIT_CARD_EXP_DT,
    T.CREDIT_CARD_STATUS = S.CREDIT_CARD_STATUS,
    T.CREDIT_CARD_AUTHCD = S.CREDIT_CARD_AUTHCD,
    T.CREDIT_CARD_DECLND = S.CREDIT_CARD_DECLND,
    T.CREDIT_CARD_ERRMSG = S.CREDIT_CARD_ERRMSG,
    T.CREDIT_CARD_VDAUTH = S.CREDIT_CARD_VDAUTH,
    T.APP_FEE_STATUS = S.APP_FEE_STATUS,
    T.APP_FEE_CALC_DTTM = S.APP_FEE_CALC_DTTM,
    T.CUR_RT_TYPE = S.CUR_RT_TYPE,
    T.RATE_MULT = S.RATE_MULT,
    T.RATE_DIV = S.RATE_DIV,
    T.ORIGNL_APPL_FEE_PD = S.ORIGNL_APPL_FEE_PD,
    T.ORIGNL_CURRENCY_CD = S.ORIGNL_CURRENCY_CD,
    T.WAIVE_AMT = S.WAIVE_AMT,
    T.SSF_IHC_PB = S.SSF_IHC_PB,
    T.UM_RA_TA_INTEREST = S.UM_RA_TA_INTEREST,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.INSTITUTION <> S.INSTITUTION or 
    T.ADM_APPL_CTR <> S.ADM_APPL_CTR or 
    T.ADMIT_TYPE <> S.ADMIT_TYPE or 
    T.FIN_AID_INTEREST <> S.FIN_AID_INTEREST or 
    T.HOUSING_INTEREST <> S.HOUSING_INTEREST or 
    T.APPL_FEE_STATUS <> S.APPL_FEE_STATUS or 
    nvl(trim(T.APPL_FEE_DT),0) <> nvl(trim(S.APPL_FEE_DT),0) or 
    T.NOTIFICATION_PLAN <> S.NOTIFICATION_PLAN or 
    T.REGION <> S.REGION or 
    T.REGION_FROM <> S.REGION_FROM or 
    T.RECRUITER_ID <> S.RECRUITER_ID or 
    T.LAST_SCH_ATTEND <> S.LAST_SCH_ATTEND or 
-- Case 46298, Aug 2020
--    T.ADM_CREATION_DT <> S.ADM_CREATION_DT or 
    T.ADM_CREATION_DT <> NVL(S.ADM_CREATION_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) or 
-- Case 46298
    T.ADM_CREATION_BY <> S.ADM_CREATION_BY or 
    nvl(trim(T.ADM_UPDATED_DT),0) <> nvl(trim(S.ADM_UPDATED_DT),0) or 
    T.ADM_UPDATED_BY <> S.ADM_UPDATED_BY or 
    T.ADM_APPL_COMPLETE <> S.ADM_APPL_COMPLETE or 
-- Case 46298, Aug 2020
--    T.ADM_APPL_DT <> S.ADM_APPL_DT
    T.ADM_APPL_DT <> NVL(S.ADM_APPL_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) or 
-- Case 46298	
    nvl(trim(T.ADM_APPL_CMPLT_DT),0) <> nvl(trim(S.ADM_APPL_CMPLT_DT),0) or 
    nvl(trim(T.GRADUATION_DT),0) <> nvl(trim(S.GRADUATION_DT),0) or 
    T.PRIOR_APPL <> S.PRIOR_APPL or 
    T.APPL_FEE_TYPE <> S.APPL_FEE_TYPE or 
    T.ADM_APPL_METHOD <> S.ADM_APPL_METHOD or 
    T.APPL_FEE_AMT <> S.APPL_FEE_AMT or 
    T.APPL_FEE_PAID <> S.APPL_FEE_PAID or 
    T.CURRENCY_CD <> S.CURRENCY_CD or 
    T.TENDER_CATEGORY <> S.TENDER_CATEGORY or 
    T.ACADEMIC_LEVEL <> S.ACADEMIC_LEVEL or 
    T.OVERRIDE_DEPOSIT <> S.OVERRIDE_DEPOSIT or 
    T.EXT_ADM_APPL_NBR <> S.EXT_ADM_APPL_NBR or 
    T.CREDIT_CARD_NBR <> S.CREDIT_CARD_NBR or 
    T.CREDIT_CARD_TYPE <> S.CREDIT_CARD_TYPE or 
    T.CREDIT_CARD_HOLDER <> S.CREDIT_CARD_HOLDER or 
    T.CREDIT_CARD_ISSUER <> S.CREDIT_CARD_ISSUER or 
    nvl(trim(T.CREDIT_CARD_EXP_DT),0) <> nvl(trim(S.CREDIT_CARD_EXP_DT),0) or 
    T.CREDIT_CARD_STATUS <> S.CREDIT_CARD_STATUS or 
    T.CREDIT_CARD_AUTHCD <> S.CREDIT_CARD_AUTHCD or 
    T.CREDIT_CARD_DECLND <> S.CREDIT_CARD_DECLND or 
    T.CREDIT_CARD_ERRMSG <> S.CREDIT_CARD_ERRMSG or 
    T.CREDIT_CARD_VDAUTH <> S.CREDIT_CARD_VDAUTH or 
    T.APP_FEE_STATUS <> S.APP_FEE_STATUS or 
    nvl(trim(T.APP_FEE_CALC_DTTM),0) <> nvl(trim(S.APP_FEE_CALC_DTTM),0) or 
    T.CUR_RT_TYPE <> S.CUR_RT_TYPE or 
    T.RATE_MULT <> S.RATE_MULT or 
    T.RATE_DIV <> S.RATE_DIV or 
    T.ORIGNL_APPL_FEE_PD <> S.ORIGNL_APPL_FEE_PD or 
    T.ORIGNL_CURRENCY_CD <> S.ORIGNL_CURRENCY_CD or 
    T.WAIVE_AMT <> S.WAIVE_AMT or 
    T.SSF_IHC_PB <> S.SSF_IHC_PB or 
    T.UM_RA_TA_INTEREST <> S.UM_RA_TA_INTEREST or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.ACAD_CAREER,
    T.STDNT_CAR_NBR,
    T.ADM_APPL_NBR, 
    T.SRC_SYS_ID, 
    T.INSTITUTION,
    T.ADM_APPL_CTR, 
    T.ADMIT_TYPE, 
    T.FIN_AID_INTEREST, 
    T.HOUSING_INTEREST, 
    T.APPL_FEE_STATUS,
    T.APPL_FEE_DT,
    T.NOTIFICATION_PLAN,
    T.REGION, 
    T.REGION_FROM,
    T.RECRUITER_ID, 
    T.LAST_SCH_ATTEND,
    T.ADM_CREATION_DT,
    T.ADM_CREATION_BY,
    T.ADM_UPDATED_DT, 
    T.ADM_UPDATED_BY, 
    T.ADM_APPL_COMPLETE,
    T.ADM_APPL_DT,
    T.ADM_APPL_CMPLT_DT,
    T.GRADUATION_DT,
    T.PRIOR_APPL, 
    T.APPL_FEE_TYPE,
    T.ADM_APPL_METHOD,
    T.APPL_FEE_AMT, 
    T.APPL_FEE_PAID,
    T.CURRENCY_CD,
    T.TENDER_CATEGORY,
    T.ACADEMIC_LEVEL, 
    T.OVERRIDE_DEPOSIT, 
    T.EXT_ADM_APPL_NBR, 
    T.CREDIT_CARD_NBR,
    T.CREDIT_CARD_TYPE, 
    T.CREDIT_CARD_HOLDER, 
    T.CREDIT_CARD_ISSUER, 
    T.CREDIT_CARD_EXP_DT, 
    T.CREDIT_CARD_STATUS, 
    T.CREDIT_CARD_AUTHCD, 
    T.CREDIT_CARD_DECLND, 
    T.CREDIT_CARD_ERRMSG, 
    T.CREDIT_CARD_VDAUTH, 
    T.APP_FEE_STATUS, 
    T.APP_FEE_CALC_DTTM,
    T.CUR_RT_TYPE,
    T.RATE_MULT,
    T.RATE_DIV, 
    T.ORIGNL_APPL_FEE_PD, 
    T.ORIGNL_CURRENCY_CD, 
    T.WAIVE_AMT,
    T.SSF_IHC_PB, 
    T.UM_RA_TA_INTEREST,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.EMPLID, 
    S.ACAD_CAREER,
    S.STDNT_CAR_NBR,
    S.ADM_APPL_NBR, 
    'CS90', 
    S.INSTITUTION,
    S.ADM_APPL_CTR, 
    S.ADMIT_TYPE, 
    S.FIN_AID_INTEREST, 
    S.HOUSING_INTEREST, 
    S.APPL_FEE_STATUS,
    NVL(S.APPL_FEE_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )),    
    S.NOTIFICATION_PLAN,
    S.REGION, 
    S.REGION_FROM,
    S.RECRUITER_ID, 
    S.LAST_SCH_ATTEND,
    NVL(S.ADM_CREATION_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )),    
    S.ADM_CREATION_BY,
    NVL(S.ADM_UPDATED_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )),    
    S.ADM_UPDATED_BY, 
    S.ADM_APPL_COMPLETE,
    NVL(S.ADM_APPL_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )),
    NVL(S.ADM_APPL_CMPLT_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )),
    NVL(S.GRADUATION_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )),
    S.PRIOR_APPL, 
    S.APPL_FEE_TYPE,
    S.ADM_APPL_METHOD,
    S.APPL_FEE_AMT, 
    S.APPL_FEE_PAID,
    S.CURRENCY_CD,
    S.TENDER_CATEGORY,
    S.ACADEMIC_LEVEL, 
    S.OVERRIDE_DEPOSIT, 
    S.EXT_ADM_APPL_NBR, 
    S.CREDIT_CARD_NBR,
    S.CREDIT_CARD_TYPE, 
    S.CREDIT_CARD_HOLDER, 
    S.CREDIT_CARD_ISSUER, 
    NVL(S.CREDIT_CARD_EXP_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )),    
    S.CREDIT_CARD_STATUS, 
    S.CREDIT_CARD_AUTHCD, 
    S.CREDIT_CARD_DECLND, 
    S.CREDIT_CARD_ERRMSG, 
    S.CREDIT_CARD_VDAUTH, 
    S.APP_FEE_STATUS, 
    NVL(S.APP_FEE_CALC_DTTM, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )),
    S.CUR_RT_TYPE,
    S.RATE_MULT,
    S.RATE_DIV, 
    S.ORIGNL_APPL_FEE_PD, 
    S.ORIGNL_CURRENCY_CD, 
    S.WAIVE_AMT,
    S.SSF_IHC_PB, 
    S.UM_RA_TA_INTEREST,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ADM_APPL_DATA rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ADM_APPL_DATA',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ADM_APPL_DATA';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ADM_APPL_DATA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ADM_APPL_DATA';
update CSSTG_OWNER.PS_ADM_APPL_DATA T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, STDNT_CAR_NBR, ADM_APPL_NBR
   from CSSTG_OWNER.PS_ADM_APPL_DATA T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APPL_DATA') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, STDNT_CAR_NBR, ADM_APPL_NBR
   from SYSADM.PS_ADM_APPL_DATA@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APPL_DATA') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.STDNT_CAR_NBR = S.STDNT_CAR_NBR
   and T.ADM_APPL_NBR = S.ADM_APPL_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ADM_APPL_DATA rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ADM_APPL_DATA',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ADM_APPL_DATA'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


EXCEPTION
    WHEN OTHERS THEN

        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END PS_ADM_APPL_DATA_P;
/

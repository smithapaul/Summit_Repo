DROP PROCEDURE CSMRT_OWNER.PS_ADM_APPL_RCR_CA_P
/

--
-- PS_ADM_APPL_RCR_CA_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.PS_ADM_APPL_RCR_CA_P AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ADM_APPL_RCR_CA from PeopleSoft table PS_ADM_APPL_RCR_CA.
--
-- V01  SMT-xxxx 09/27/2017,    Jim Doucette
--                              Converted from DS PS_ADM_APPL_RCR_CA
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ADM_APPL_RCR_CA';
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
       START_DT = SYSDATE,
       END_DT = NULL
 where TABLE_NAME = 'PS_ADM_APPL_RCR_CA'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ADM_APPL_RCR_CA@SASOURCE S)
 where TABLE_NAME = 'PS_ADM_APPL_RCR_CA'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table CSSTG_OWNER.PS_T_ADM_APPL_RCR_CA';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strSqlCommand   := 'Loading temp table for CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Loading'
 where TABLE_NAME = 'PS_ADM_APPL_RCR_CA'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
insert /*+ append */  into CSSTG_OWNER.PS_T_ADM_APPL_RCR_CA
select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(STDNT_CAR_NBR,0) STDNT_CAR_NBR, 
    nvl(trim(ADM_APPL_NBR),'-') ADM_APPL_NBR, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(RECRUITMENT_CAT),'-') RECRUITMENT_CAT, 
    nvl(trim(RECRUIT_SUB_CAT),'-') RECRUIT_SUB_CAT, 
    to_char(substr(trim(DESCRLONG), 1, 4000)) DESCRLONG,
    to_number(ORA_ROWSCN) SRC_SCN
from SYSADM.PS_ADM_APPL_RCR_CA@SASOURCE S 
where EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_ADM_APPL_RCR_CA'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ADM_APPL_RCR_CA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ADM_APPL_RCR_CA';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ADM_APPL_RCR_CA T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(STDNT_CAR_NBR,0) STDNT_CAR_NBR, 
    nvl(trim(ADM_APPL_NBR),'-') ADM_APPL_NBR, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(RECRUITMENT_CAT),'-') RECRUITMENT_CAT, 
    nvl(trim(RECRUIT_SUB_CAT),'-') RECRUIT_SUB_CAT, 
    DESCRLONG
from CSSTG_OWNER.PS_T_ADM_APPL_RCR_CA S 
where SRC_SCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APPL_RCR_CA')
  and EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8 ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.STDNT_CAR_NBR = S.STDNT_CAR_NBR and 
    T.ADM_APPL_NBR = S.ADM_APPL_NBR and 
    T.INSTITUTION = S.INSTITUTION and 
    T.RECRUITMENT_CAT = S.RECRUITMENT_CAT and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.RECRUIT_SUB_CAT = S.RECRUIT_SUB_CAT,
    T.DESCRLONG = S.DESCRLONG,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.RECRUIT_SUB_CAT <> S.RECRUIT_SUB_CAT or 
    nvl(trim(T.DESCRLONG),0) <> nvl(trim(S.DESCRLONG),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.ACAD_CAREER,
    T.STDNT_CAR_NBR,
    T.ADM_APPL_NBR, 
    T.INSTITUTION,
    T.RECRUITMENT_CAT,
    T.SRC_SYS_ID, 
    T.RECRUIT_SUB_CAT,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID,
    T.DESCRLONG
    ) 
values (
    S.EMPLID, 
    S.ACAD_CAREER,
    S.STDNT_CAR_NBR,
    S.ADM_APPL_NBR, 
    S.INSTITUTION,
    S.RECRUITMENT_CAT,
    'CS90', 
    S.RECRUIT_SUB_CAT,
    'N',
    'S',
    sysdate,
    sysdate,
    1234,
    S.DESCRLONG)
;


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := '# of PS_ADM_APPL_RCR_CA rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ADM_APPL_RCR_CA',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ADM_APPL_RCR_CA';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ADM_APPL_RCR_CA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ADM_APPL_RCR_CA';
update CSSTG_OWNER.PS_ADM_APPL_RCR_CA T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, STDNT_CAR_NBR, ADM_APPL_NBR, INSTITUTION, RECRUITMENT_CAT
   from CSSTG_OWNER.PS_ADM_APPL_RCR_CA T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APPL_RCR_CA') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, STDNT_CAR_NBR, ADM_APPL_NBR, INSTITUTION, RECRUITMENT_CAT
   from SYSADM.PS_ADM_APPL_RCR_CA@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APPL_RCR_CA') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.STDNT_CAR_NBR = S.STDNT_CAR_NBR
   and T.ADM_APPL_NBR = S.ADM_APPL_NBR
   and T.INSTITUTION = S.INSTITUTION
   and T.RECRUITMENT_CAT = S.RECRUITMENT_CAT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ADM_APPL_RCR_CA rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ADM_APPL_RCR_CA',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ADM_APPL_RCR_CA'
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

END PS_ADM_APPL_RCR_CA_P;
/

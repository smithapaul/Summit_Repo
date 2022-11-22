DROP PROCEDURE CSMRT_OWNER.PS_UM_PRSPCT_REFL_P
/

--
-- PS_UM_PRSPCT_REFL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_UM_PRSPCT_REFL_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_UM_PRSPCT_REFL from PeopleSoft table PS_UM_PRSPCT_REFL.
--
-- V01  SMT-xxxx 10/10/2017,    James Doucette
--                              Converted from DataStage
-- V02  SMT-xxxx 10/19/2017,    James Doucette
--                              Added new unique key field from SA
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_UM_PRSPCT_REFL';
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
 where TABLE_NAME = 'PS_UM_PRSPCT_REFL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_UM_PRSPCT_REFL@SASOURCE S)
 where TABLE_NAME = 'PS_UM_PRSPCT_REFL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_UM_PRSPCT_REFL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_UM_PRSPCT_REFL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_UM_PRSPCT_REFL T 
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(UM_REFRL_GRP),'-') UM_REFRL_GRP, 
    nvl(trim(UM_REFRL_DTL),'-') UM_REFRL_DTL, 
    NVL(UM_REFRL_DATE, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) UM_REFRL_DATE, 
    nvl(trim(ADMIT_TERM),'-') ADMIT_TERM,
    nvl(trim(UM_ADM_REC_NBR),'-') UM_ADM_REC_NBR,
    nvl(trim(ADM_RECR_CTR),'-') ADM_RECR_CTR
from SYSADM.PS_UM_PRSPCT_REFL@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_PRSPCT_REFL')
  and EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8 ) S 
 on ( 
    T.EMPLID = S.EMPLID and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.INSTITUTION = S.INSTITUTION and 
    T.UM_REFRL_GRP = S.UM_REFRL_GRP and 
    T.UM_REFRL_DTL = S.UM_REFRL_DTL and 
    T.UM_REFRL_DATE = S.UM_REFRL_DATE and 
    T.ADMIT_TERM = S.ADMIT_TERM and
    T.UM_ADM_REC_NBR = S.UM_ADM_REC_NBR and
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.ADM_RECR_CTR = S.ADM_RECR_CTR,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.ADM_RECR_CTR <> S.ADM_RECR_CTR or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.ACAD_CAREER,
    T.INSTITUTION,
    T.UM_REFRL_GRP, 
    T.UM_REFRL_DTL, 
    T.UM_REFRL_DATE,
    T.ADMIT_TERM,
    T.UM_ADM_REC_NBR,    
    T.SRC_SYS_ID,
    T.ADM_RECR_CTR, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.EMPLID, 
    S.ACAD_CAREER,
    S.INSTITUTION,
    S.UM_REFRL_GRP, 
    S.UM_REFRL_DTL,  
    S.UM_REFRL_DATE,
    S.ADMIT_TERM, 
    S.UM_ADM_REC_NBR,
    'CS90',
    S.ADM_RECR_CTR, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234)
;


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_PRSPCT_REFL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_PRSPCT_REFL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_UM_PRSPCT_REFL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_UM_PRSPCT_REFL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_UM_PRSPCT_REFL';
update CSSTG_OWNER.PS_UM_PRSPCT_REFL T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, INSTITUTION, UM_REFRL_GRP, UM_REFRL_DTL, UM_REFRL_DATE, ADMIT_TERM, UM_ADM_REC_NBR 
   from CSSTG_OWNER.PS_UM_PRSPCT_REFL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_PRSPCT_REFL') = 'Y'
  minus
 select nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(UM_REFRL_GRP),'-') UM_REFRL_GRP, 
    nvl(trim(UM_REFRL_DTL),'-') UM_REFRL_DTL, 
    NVL(UM_REFRL_DATE, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) UM_REFRL_DATE, 
    nvl(trim(ADMIT_TERM),'-') ADMIT_TERM,
    nvl(trim(UM_ADM_REC_NBR),'-') UM_ADM_REC_NBR
   from SYSADM.PS_UM_PRSPCT_REFL@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_PRSPCT_REFL') = 'Y' 
   ) S
 where T.EMPLID = S.EMPLID
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.INSTITUTION = S.INSTITUTION
   and T.UM_REFRL_GRP = S.UM_REFRL_GRP
   and T.UM_REFRL_DTL = S.UM_REFRL_DTL
   and T.UM_REFRL_DATE = S.UM_REFRL_DATE
   and T.ADMIT_TERM = S.ADMIT_TERM
   and T.UM_ADM_REC_NBR = S.UM_ADM_REC_NBR
   and T.SRC_SYS_ID = 'CS90'    
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_PRSPCT_REFL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_PRSPCT_REFL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_UM_PRSPCT_REFL'
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

END PS_UM_PRSPCT_REFL_P;
/

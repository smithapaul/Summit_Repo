DROP PROCEDURE CSMRT_OWNER.PS_TRNSFR_TO_P
/

--
-- PS_TRNSFR_TO_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_TRNSFR_TO_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_TRNSFR_TO from PeopleSoft table PS_TRNSFR_TO.
--
 --V01  SMT-xxxx 10/06/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_TRNSFR_TO';
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
 where TABLE_NAME = 'PS_TRNSFR_TO'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_TRNSFR_TO@SASOURCE S)
 where TABLE_NAME = 'PS_TRNSFR_TO'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_TRNSFR_TO';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_TRNSFR_TO';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_TRNSFR_TO T
using (select /*+ full(S) */
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(TRNSFR_SRC_ID),'-') TRNSFR_SRC_ID, 
    nvl(trim(COMP_SUBJECT_AREA),'-') COMP_SUBJECT_AREA, 
    EFFDT, 
    nvl(trim(TRNSFR_EQVLNCY_CMP),'-') TRNSFR_EQVLNCY_CMP, 
    nvl(trim(CRSE_ID),'-') CRSE_ID, 
    nvl(CRSE_OFFER_NBR,0) CRSE_OFFER_NBR, 
    nvl(UNT_TAKEN,0) UNT_TAKEN, 
    nvl(trim(SSR_TR_DEF_GRD_TYP),'-') SSR_TR_DEF_GRD_TYP, 
    nvl(trim(SSR_TR_DEF_GRD_SEQ),'-') SSR_TR_DEF_GRD_SEQ
from SYSADM.PS_TRNSFR_TO@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNSFR_TO') ) S
 on ( 
    T.INSTITUTION = S.INSTITUTION and 
    T.TRNSFR_SRC_ID = S.TRNSFR_SRC_ID and 
    T.COMP_SUBJECT_AREA = S.COMP_SUBJECT_AREA and 
    T.EFFDT = S.EFFDT and 
    T.TRNSFR_EQVLNCY_CMP = S.TRNSFR_EQVLNCY_CMP and 
    T.CRSE_ID = S.CRSE_ID and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR,
    T.UNT_TAKEN = S.UNT_TAKEN,
    T.SSR_TR_DEF_GRD_TYP = S.SSR_TR_DEF_GRD_TYP,
    T.SSR_TR_DEF_GRD_SEQ = S.SSR_TR_DEF_GRD_SEQ,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.CRSE_OFFER_NBR <> S.CRSE_OFFER_NBR or 
    T.UNT_TAKEN <> S.UNT_TAKEN or 
    T.SSR_TR_DEF_GRD_TYP <> S.SSR_TR_DEF_GRD_TYP or 
    T.SSR_TR_DEF_GRD_SEQ <> S.SSR_TR_DEF_GRD_SEQ or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.INSTITUTION,
    T.TRNSFR_SRC_ID,
    T.COMP_SUBJECT_AREA,
    T.EFFDT,
    T.TRNSFR_EQVLNCY_CMP, 
    T.CRSE_ID,
    T.SRC_SYS_ID, 
    T.CRSE_OFFER_NBR, 
    T.UNT_TAKEN,
    T.SSR_TR_DEF_GRD_TYP, 
    T.SSR_TR_DEF_GRD_SEQ, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.INSTITUTION,
    S.TRNSFR_SRC_ID,
    S.COMP_SUBJECT_AREA,
    S.EFFDT,
    S.TRNSFR_EQVLNCY_CMP, 
    S.CRSE_ID,
    'CS90', 
    S.CRSE_OFFER_NBR, 
    S.UNT_TAKEN,
    S.SSR_TR_DEF_GRD_TYP, 
    S.SSR_TR_DEF_GRD_SEQ, 
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

strMessage01    := '# of PS_TRNSFR_TO rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TRNSFR_TO',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_TRNSFR_TO';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_TRNSFR_TO';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_TRNSFR_TO';
update CSSTG_OWNER.PS_TRNSFR_TO T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, TRNSFR_SRC_ID, COMP_SUBJECT_AREA, EFFDT, TRNSFR_EQVLNCY_CMP, CRSE_ID
   from CSSTG_OWNER.PS_TRNSFR_TO T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNSFR_TO') = 'Y'
  minus
 select INSTITUTION, TRNSFR_SRC_ID, COMP_SUBJECT_AREA, EFFDT, TRNSFR_EQVLNCY_CMP, CRSE_ID
   from SYSADM.PS_TRNSFR_TO@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNSFR_TO') = 'Y' 
   ) S
 where T.INSTITUTION = S.INSTITUTION
   and T.TRNSFR_SRC_ID = S.TRNSFR_SRC_ID
   and T.COMP_SUBJECT_AREA = S.COMP_SUBJECT_AREA
   and T.EFFDT = S.EFFDT
   and T.TRNSFR_EQVLNCY_CMP = S.TRNSFR_EQVLNCY_CMP
   and T.CRSE_ID = S.CRSE_ID
   and T.SRC_SYS_ID = 'CS90'    
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_TRNSFR_TO rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TRNSFR_TO',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_TRNSFR_TO'
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

END PS_TRNSFR_TO_P;
/

DROP PROCEDURE CSMRT_OWNER.PS_UM_STRZ_HSGDTL_P
/

--
-- PS_UM_STRZ_HSGDTL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_UM_STRZ_HSGDTL_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_UM_STRZ_HSGDTL from PeopleSoft table PS_UM_STRZ_HSGDTL.
--
 --V01  Case-24779 03/26/2020,    James Doucette
--                                New Stage Table.
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_UM_STRZ_HSGDTL';
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
 where TABLE_NAME = 'PS_UM_STRZ_HSGDTL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_UM_STRZ_HSGDTL@SASOURCE S)
 where TABLE_NAME = 'PS_UM_STRZ_HSGDTL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_UM_STRZ_HSGDTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_UM_STRZ_HSGDTL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_UM_STRZ_HSGDTL T 
using (select /*+ full(S) */
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(EMPLID),'-') EMPLID, 
	nvl(trim(COMMENT1),'-') COMMENT1, 
    APPLIES_AT_DATE, 
	COMPLETED_DT,
	AGREEMENT_DATE,
    nvl(trim(COMMENTS_256),'-') COMMENTS_256, 
	DATE_LOADED
from SYSADM.PS_UM_STRZ_HSGDTL@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_STRZ_HSGDTL') ) S 
 on ( 
    T.INSTITUTION = S.INSTITUTION and 
    T.EMPLID = S.EMPLID and 
    T.COMMENT1 = nvl(trim(S.COMMENT1),'-') and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.APPLIES_AT_DATE = S.APPLIES_AT_DATE,
    T.COMPLETED_DT = S.COMPLETED_DT,
    T.AGREEMENT_DATE = S.AGREEMENT_DATE,
    T.COMMENTS_256 = S.COMMENTS_256,
    T.DATE_LOADED = S.DATE_LOADED,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate
where 
    T.APPLIES_AT_DATE <> S.APPLIES_AT_DATE or 
    T.COMPLETED_DT <> S.COMPLETED_DT or 
    T.AGREEMENT_DATE <> S.AGREEMENT_DATE or 
    T.COMMENTS_256 <> S.COMMENTS_256 or 
    T.DATE_LOADED <> S.DATE_LOADED or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.INSTITUTION,
    T.EMPLID,
    T.COMMENT1,
    T.SRC_SYS_ID, 
    T.APPLIES_AT_DATE, 
    T.COMPLETED_DT,
    T.AGREEMENT_DATE, 
    T.COMMENTS_256,
	T.DATE_LOADED,
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM
    ) 
values (
    S.INSTITUTION,
    S.EMPLID,
    S.COMMENT1,
    'CS90', 
    S.APPLIES_AT_DATE, 
    S.COMPLETED_DT,
    S.AGREEMENT_DATE, 
    S.COMMENTS_256,
	S.DATE_LOADED,
    'S',
    sysdate,
    sysdate)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_STRZ_HSGDTL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_STRZ_HSGDTL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_UM_STRZ_HSGDTL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_UM_STRZ_HSGDTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_UM_STRZ_HSGDTL';
update CSSTG_OWNER.PS_UM_STRZ_HSGDTL T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, EMPLID, nvl(trim(COMMENT1),'-') COMMENT1
   from CSSTG_OWNER.PS_UM_STRZ_HSGDTL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_STRZ_HSGDTL') = 'Y'
  minus
 select nvl(trim(INSTITUTION),'-') INSTITUTION, nvl(trim(EMPLID),'-') EMPLID, nvl(trim(COMMENT1),'-') COMMENT1
   from SYSADM.PS_UM_STRZ_HSGDTL@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_STRZ_HSGDTL') = 'Y' 
   ) S
 where T.INSTITUTION = S.INSTITUTION   
   and T.EMPLID = S.EMPLID
   and T.COMMENT1 = S.COMMENT1
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_STRZ_HSGDTL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_STRZ_HSGDTL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_UM_STRZ_HSGDTL'
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

END PS_UM_STRZ_HSGDTL_P;
/

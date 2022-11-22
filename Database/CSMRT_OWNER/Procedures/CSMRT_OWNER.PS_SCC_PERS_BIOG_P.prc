DROP PROCEDURE CSMRT_OWNER.PS_SCC_PERS_BIOG_P
/

--
-- PS_SCC_PERS_BIOG_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_SCC_PERS_BIOG_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--
-- Loads stage table PS_SCC_PERS_BIOG from PeopleSoft table PS_SCC_PERS_BIOG.
--
 --V01  SMT-xxxx 10/25/2022,    Steve Celi
--                              
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_SCC_PERS_BIOG';
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
 where TABLE_NAME = 'PS_SCC_PERS_BIOG'      -- Changed 
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_SCC_PERS_BIOG@SASOURCE S)
 where TABLE_NAME = 'PS_SCC_PERS_BIOG'
;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Merging data into CSSTG_OWNER.PS_SCC_PERS_BIOG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_SCC_PERS_BIOG';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_SCC_PERS_BIOG T
using (select /*+ full(S) */
EMPLID,
EFFDT,
SCC_BIRTH_GENDER,
SCC_GENDER_ID,
SCC_GENDER_ID_OTH,
SCC_SEXUAL_ORT,
SCC_SEXUAL_ORT_OTH,
SCC_PRONOUNS,
SCC_PRONOUNS_OTH,
SCC_BIO_ATTRB_1,
SCC_BIO_ATTRB_2,
SCC_BIO_ATTRB_3,
SCC_BIO_ATTRB_4,
SCC_BIO_ATTRB_5,
SCC_BIO_ATTRB_6,
SCC_BIO_ATTRB_7,
SCC_BIO_ATTRB_8,
SCC_BIO_ATTRB_9,
LASTUPDDTTM,
LASTUPDOPRID
from SYSADM.PS_SCC_PERS_BIOG@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SCC_PERS_BIOG') 
and
trim(
       SCC_BIRTH_GENDER||
       SCC_GENDER_ID||
       SCC_GENDER_ID_OTH||
       SCC_SEXUAL_ORT||
       SCC_SEXUAL_ORT_OTH||
       SCC_PRONOUNS||
       SCC_PRONOUNS_OTH||
       SCC_BIO_ATTRB_1||
       SCC_BIO_ATTRB_2||
       SCC_BIO_ATTRB_3||
       SCC_BIO_ATTRB_4||
       SCC_BIO_ATTRB_5||
       SCC_BIO_ATTRB_6||
       SCC_BIO_ATTRB_7||
       SCC_BIO_ATTRB_8||
       SCC_BIO_ATTRB_9) is not NULL
) S
   on (
T.EMPLID = S.EMPLID and
T.EFFDT=S.EFFDT and     -- Added
T.SRC_SYS_ID = 'CS90')
when matched then update set
--T.EFFDT=S.EFFDT,      -- Changed
T.SCC_BIRTH_GENDER=S.SCC_BIRTH_GENDER,
T.SCC_GENDER_ID=S.SCC_GENDER_ID,
T.SCC_GENDER_ID_OTH=S.SCC_GENDER_ID_OTH,
T.SCC_SEXUAL_ORT=S.SCC_SEXUAL_ORT,
T.SCC_SEXUAL_ORT_OTH=S.SCC_SEXUAL_ORT_OTH,
T.SCC_PRONOUNS=S.SCC_PRONOUNS,
T.SCC_PRONOUNS_OTH=S.SCC_PRONOUNS_OTH,
T.SCC_BIO_ATTRB_1=S.SCC_BIO_ATTRB_1,
T.SCC_BIO_ATTRB_2=S.SCC_BIO_ATTRB_2,
T.SCC_BIO_ATTRB_3=S.SCC_BIO_ATTRB_3,
T.SCC_BIO_ATTRB_4=S.SCC_BIO_ATTRB_4,
T.SCC_BIO_ATTRB_5=S.SCC_BIO_ATTRB_5,
T.SCC_BIO_ATTRB_6=S.SCC_BIO_ATTRB_6,
T.SCC_BIO_ATTRB_7=S.SCC_BIO_ATTRB_7,
T.SCC_BIO_ATTRB_8=S.SCC_BIO_ATTRB_8,
T.SCC_BIO_ATTRB_9=S.SCC_BIO_ATTRB_9,
T.LASTUPDDTTM=S.LASTUPDDTTM,
T.LASTUPDOPRID=S.LASTUPDOPRID,
T.DATA_ORIGIN='S',
T.LASTUPD_EW_DTTM=SYSDATE
where
--decode(T.EFFDT,S.EFFDT,0,1) = 1 or    -- Changed 
decode(T.SCC_BIRTH_GENDER,S.SCC_BIRTH_GENDER,0,1) = 1 or 
decode(T.SCC_GENDER_ID,S.SCC_GENDER_ID,0,1) = 1 or 
decode(T.SCC_GENDER_ID_OTH,S.SCC_GENDER_ID_OTH,0,1) = 1 or 
decode(T.SCC_SEXUAL_ORT,S.SCC_SEXUAL_ORT,0,1) = 1 or 
decode(T.SCC_SEXUAL_ORT_OTH,S.SCC_SEXUAL_ORT_OTH,0,1) = 1 or 
decode(T.SCC_PRONOUNS,S.SCC_PRONOUNS,0,1) = 1 or 
decode(T.SCC_PRONOUNS_OTH,S.SCC_PRONOUNS_OTH,0,1) = 1 or 
decode(T.SCC_BIO_ATTRB_1,S.SCC_BIO_ATTRB_1,0,1) = 1 or 
decode(T.SCC_BIO_ATTRB_2,S.SCC_BIO_ATTRB_2,0,1) = 1 or 
decode(T.SCC_BIO_ATTRB_3,S.SCC_BIO_ATTRB_3,0,1) = 1 or 
decode(T.SCC_BIO_ATTRB_4,S.SCC_BIO_ATTRB_4,0,1) = 1 or 
decode(T.SCC_BIO_ATTRB_5,S.SCC_BIO_ATTRB_5,0,1) = 1 or 
decode(T.SCC_BIO_ATTRB_6,S.SCC_BIO_ATTRB_6,0,1) = 1 or 
decode(T.SCC_BIO_ATTRB_7,S.SCC_BIO_ATTRB_7,0,1) = 1 or 
decode(T.SCC_BIO_ATTRB_8,S.SCC_BIO_ATTRB_8,0,1) = 1 or 
decode(T.SCC_BIO_ATTRB_9,S.SCC_BIO_ATTRB_9,0,1) = 1 or 
decode(T.LASTUPDDTTM,S.LASTUPDDTTM,0,1) = 1 or 
decode(T.LASTUPDOPRID,S.LASTUPDOPRID,0,1) = 1 or 
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.EFFDT,
T.SRC_SYS_ID,
T.SCC_BIRTH_GENDER,
T.SCC_GENDER_ID,
T.SCC_GENDER_ID_OTH,
T.SCC_SEXUAL_ORT,
T.SCC_SEXUAL_ORT_OTH,
T.SCC_PRONOUNS,
T.SCC_PRONOUNS_OTH,
T.SCC_BIO_ATTRB_1,
T.SCC_BIO_ATTRB_2,
T.SCC_BIO_ATTRB_3,
T.SCC_BIO_ATTRB_4,
T.SCC_BIO_ATTRB_5,
T.SCC_BIO_ATTRB_6,
T.SCC_BIO_ATTRB_7,
T.SCC_BIO_ATTRB_8,
T.SCC_BIO_ATTRB_9,
T.LASTUPDDTTM,
T.LASTUPDOPRID,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM
)
values (
S.EMPLID,
S.EFFDT,
'CS90',
S.SCC_BIRTH_GENDER,
S.SCC_GENDER_ID,
S.SCC_GENDER_ID_OTH,
S.SCC_SEXUAL_ORT,
S.SCC_SEXUAL_ORT_OTH,
S.SCC_PRONOUNS,
S.SCC_PRONOUNS_OTH,
S.SCC_BIO_ATTRB_1,
S.SCC_BIO_ATTRB_2,
S.SCC_BIO_ATTRB_3,
S.SCC_BIO_ATTRB_4,
S.SCC_BIO_ATTRB_5,
S.SCC_BIO_ATTRB_6,
S.SCC_BIO_ATTRB_7,
S.SCC_BIO_ATTRB_8,
S.SCC_BIO_ATTRB_9,
S.LASTUPDDTTM,
S.LASTUPDOPRID,
'S',
SYSDATE,
SYSDATE);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_SCC_PERS_BIOG rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SCC_PERS_BIOG',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_SCC_PERS_BIOG';

strSqlCommand := 'commit';
commit;

strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_SCC_PERS_BIOG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_SCC_PERS_BIOG';    -- Changed 
update CSSTG_OWNER.PS_SCC_PERS_BIOG T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, EFFDT
   from CSSTG_OWNER.PS_SCC_PERS_BIOG T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SCC_PERS_BIOG') = 'Y'
  minus
 select EMPLID, EFFDT
   from SYSADM.PS_SCC_PERS_BIOG@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SCC_PERS_BIOG') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_SCC_PERS_BIOG rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SCC_PERS_BIOG',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_SCC_PERS_BIOG'
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

END PS_SCC_PERS_BIOG_P;
/

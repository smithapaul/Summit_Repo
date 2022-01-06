CREATE OR REPLACE PROCEDURE             "UM_F_ADM_APPL_LST_SCHL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
-- Loads table                -- UM_F_ADM_APPL_LST_SCHL
-- V01 12/12/2018             -- srikanth ,pabbu converted to proc from sql scripts
-- V01.2  SMT-8300 09/06/2017,    James Doucette
--                                Added two new fields and housekeeping fields.

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_ADM_APPL_LST_SCHL';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_LST_SCHL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_ADM_APPL_LST_SCHL');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_LST_SCHL disable constraint PK_UM_F_ADM_APPL_LST_SCHL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_ADM_APPL_LST_SCHL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_ADM_APPL_LST_SCHL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_ADM_APPL_LST_SCHL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_ADM_APPL_LST_SCHL';				
insert /*+ append */ into UM_F_ADM_APPL_LST_SCHL
with 
ADM as (
select /*+ INLINE PARALLEL(8) */ distinct
       APPLCNT_SID,
       INSTITUTION_SID,
       LST_SCHL_ATTND_SID,
       SRC_SYS_ID,
       PERSON_ID,
       INSTITUTION_CD
  from UM_F_ADM_APPL_STAT
 --where PERSON_ID = '01435020'
 ),                 -- Temp!!! EXT as ( 
EXT as (
select /*+ INLINE PARALLEL(8) */ 
       PERSON_SID, 
       INSTITUTION_SID, 
       EXT_ORG_SID, 
       SRC_SYS_ID, 
       PERSON_ID,               -- Move to UM_F_ADM_APPL_STAT!!!  
       INSTITUTION_CD,          -- Move to UM_F_ADM_APPL_STAT!!!  
       EXT_ORG_ID,              -- Move to UM_F_ADM_APPL_STAT!!!   
       EXT_DATA_NBR,            -- Added Sept 2015 
       EXT_ACAD_CAR_ID,         -- Added Sept 2015 
       EXT_SUMM_TYPE_ID,        -- Added Sept 2015 
       CLASS_RANK, 
       CLASS_SIZE, 
       CLASS_PERCENTILE, 
       EXT_GPA, 
       CONVERTED_GPA,
       UM_CUM_CREDIT, 
       UM_CUM_GPA, 
       UM_CUM_QP, 
       UM_GPA_EXCLUDE_FLG, 
       UM_EXT_ORG_CR, 
       UM_EXT_ORG_QP, 
       UM_EXT_ORG_GPA, 
       UM_EXT_ORG_CNV_CR, 
       UM_EXT_ORG_CNV_GPA, 
       UM_EXT_ORG_CNV_QP, 
       UM_GPA_OVRD_FLG, 
       UM_1_OVRD_HSGPA_FLG, 
       UM_CONVERT_GPA,
	   UM_EXT_OR_MTSC_GPA,       -- SMT-8300  
       MS_CONVERT_GPA,           -- SMT-8300  
       DATA_ORIGIN,              -- SMT-8300
       CREATED_EW_DTTM,          -- SMT-8300
       LASTUPD_EW_DTTM,           -- SMT-8300
       row_number() over (partition by PERSON_SID, INSTITUTION_SID, EXT_ORG_SID, SRC_SYS_ID 
                              order by CONVERTED_GPA desc, EXT_SUMM_TYPE_ID desc, EXT_ACAD_CAR_ID desc, EXT_DATA_NBR) EXT_ORDER   -- Aug 2019    
  from PS_F_EXT_ACAD_SUMM
 where DATA_ORIGIN <> 'D'
   )
select ADM.APPLCNT_SID,
       ADM.INSTITUTION_SID,
       ADM.LST_SCHL_ATTND_SID EXT_ORG_SID,
       ADM.SRC_SYS_ID,
       ADM.PERSON_ID, 
       ADM.INSTITUTION_CD,
       nvl(EXT.EXT_ORG_ID,'-') EXT_ORG_ID, 
       nvl(EXT.EXT_DATA_NBR,0) EXT_DATA_NBR,            -- Added Sept 2015 
       nvl(EXT.EXT_ACAD_CAR_ID,'-') EXT_ACAD_CAR_ID,    -- Added Sept 2015 
       nvl(EXT.EXT_SUMM_TYPE_ID,'-') EXT_SUMM_TYPE_ID,  -- Added Sept 2015 
       decode(EXT.CLASS_RANK,0,NULL,EXT.CLASS_RANK) CLASS_RANK,
       decode(EXT.CLASS_SIZE,0,NULL,EXT.CLASS_SIZE) CLASS_SIZE,
       decode(EXT.CLASS_PERCENTILE,0,NULL,EXT.CLASS_PERCENTILE) CLASS_PERCENTILE,
       decode(EXT.EXT_GPA,0,NULL,EXT.EXT_GPA) EXT_GPA,
       decode(EXT.CONVERTED_GPA,0,NULL,EXT.CONVERTED_GPA) CONVERTED_GPA,
       decode(EXT.UM_CUM_CREDIT,0,NULL,EXT.UM_CUM_CREDIT) UM_CUM_CREDIT,
       decode(EXT.UM_CUM_GPA,0,NULL,EXT.UM_CUM_GPA) UM_CUM_GPA,
       decode(EXT.UM_CUM_QP,0,NULL,EXT.UM_CUM_QP) UM_CUM_QP,
       nvl(EXT.UM_GPA_EXCLUDE_FLG,'-') UM_GPA_EXCLUDE_FLG,
       decode(EXT.UM_EXT_ORG_CR,0,NULL,EXT.UM_EXT_ORG_CR) UM_EXT_ORG_CR,
       decode(EXT.UM_EXT_ORG_QP,0,NULL,EXT.UM_EXT_ORG_QP) UM_EXT_ORG_QP,
       decode(EXT.UM_EXT_ORG_GPA,0,NULL,EXT.UM_EXT_ORG_GPA) UM_EXT_ORG_GPA,
       decode(EXT.UM_EXT_ORG_CNV_CR,0,NULL,EXT.UM_EXT_ORG_CNV_CR) UM_EXT_ORG_CNV_CR,
       decode(EXT.UM_EXT_ORG_CNV_GPA,0,NULL,EXT.UM_EXT_ORG_CNV_GPA) UM_EXT_ORG_CNV_GPA,
       decode(EXT.UM_EXT_ORG_CNV_QP,0,NULL,EXT.UM_EXT_ORG_CNV_QP) UM_EXT_ORG_CNV_QP,
       nvl(EXT.UM_GPA_OVRD_FLG,'-') UM_GPA_OVRD_FLG,
       nvl(EXT.UM_1_OVRD_HSGPA_FLG,'-') UM_1_OVRD_HSGPA_FLG,
       decode(EXT.UM_CONVERT_GPA,0,NULL,EXT.UM_CONVERT_GPA) UM_CONVERT_GPA, 
	   decode(EXT.UM_EXT_OR_MTSC_GPA,0,NULL,EXT.UM_EXT_OR_MTSC_GPA) UM_EXT_OR_MTSC_GPA,       -- SMT-8300  
       decode(EXT.MS_CONVERT_GPA,0,NULL,EXT.MS_CONVERT_GPA) MS_CONVERT_GPA,           -- SMT-8300  
       'S',              -- SMT-8300
       sysdate,          -- SMT-8300
       sysdate           -- SMT-8300
  from ADM
  left outer join EXT
    on ADM.APPLCNT_SID = EXT.PERSON_SID
   and ADM.SRC_SYS_ID = EXT.SRC_SYS_ID
   and ADM.LST_SCHL_ATTND_SID = EXT.EXT_ORG_SID
   and ADM.INSTITUTION_SID = EXT.INSTITUTION_SID
   and EXT.EXT_ORDER = 1
 where ADM.APPLCNT_SID <> 2147483646         	-- Aug 2018 
   and ADM.INSTITUTION_SID <> 2147483646     	-- Aug 2018 
--   and ADM.LST_SCHL_ATTND_SID <> 2147483646     -- Aug 2018 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_ADM_APPL_LST_SCHL rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_ADM_APPL_LST_SCHL',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_LST_SCHL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_LST_SCHL enable constraint PK_UM_F_ADM_APPL_LST_SCHL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_ADM_APPL_LST_SCHL');

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

END UM_F_ADM_APPL_LST_SCHL_P;
/

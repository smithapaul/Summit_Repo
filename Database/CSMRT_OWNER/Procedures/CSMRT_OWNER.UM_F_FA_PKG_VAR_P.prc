CREATE OR REPLACE PROCEDURE             "UM_F_FA_PKG_VAR_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_FA_PKG_VAR.
--
--V01   SMT-xxxx 07/26/2018,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_PKG_VAR';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_PKG_VAR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_PKG_VAR');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_PKG_VAR disable constraint PK_UM_F_FA_PKG_VAR';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_PKG_VAR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_PKG_VAR';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_PKG_VAR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_PKG_VAR';				
insert /*+ append */ into UM_F_FA_PKG_VAR 
SELECT /*+ PARALLEL(8) INLINE */
       INSTITUTION,
       ACAD_CAREER,
	   AID_YEAR,
       EMPLID,	   
       V.SRC_SYS_ID,
       nvl(I.INSTITUTION_SID,2147483646) INSTITUTION_SID,
       nvl(C.ACAD_CAR_SID, 2147483646) EXT_ACAD_CAR_SID, 
       nvl(P.PERSON_SID, 2147483646) PERSON_SID,
       VARIABLE_CHAR1,
       VARIABLE_CHAR2,
       VARIABLE_CHAR3,
       VARIABLE_CHAR4,
       VARIABLE_CHAR5,
       VARIABLE_CHAR6,
       VARIABLE_CHAR7,
       VARIABLE_CHAR8,
       VARIABLE_CHAR9,
	   VARIABLE_CHAR10,
       VARIABLE_FLAG1,
       VARIABLE_FLAG2,
       VARIABLE_FLAG3,
       VARIABLE_FLAG4,
       VARIABLE_FLAG5,
       VARIABLE_FLAG6,
       VARIABLE_FLAG7,
       VARIABLE_FLAG8,
       VARIABLE_FLAG9,
	   VARIABLE_FLAG10,
       VARIABLE_NUM1,
       VARIABLE_NUM2,
       VARIABLE_NUM3,
       VARIABLE_NUM4,
       VARIABLE_NUM5,
       VARIABLE_NUM6,
       VARIABLE_NUM7,
       VARIABLE_NUM8,
       VARIABLE_NUM9,
	   VARIABLE_NUM10,
       RATING_CMP1,
       RATING_CMP2,
       RATING_CMP3,
       RATING_CMP4,
       RATING_CMP5,
       RATING_CMP6,
       RATING_CMP7,
       RATING_CMP8,
       RATING_CMP9,
       RATING_CMP10,
       RATING_CMP11,
       RATING_CMP12,
       RATING_CMP13,
       RATING_CMP14,
       RATING_CMP15,
       RATING_CMP16,
       RATING_CMP17,
       RATING_CMP18,
       RATING_CMP19,
       RATING_CMP20,
       RATING_CMP_VALUE1,
       RATING_CMP_VALUE2,
       RATING_CMP_VALUE3,
       RATING_CMP_VALUE4,
       RATING_CMP_VALUE5,
       RATING_CMP_VALUE6,
       RATING_CMP_VALUE7,
       RATING_CMP_VALUE8,
       RATING_CMP_VALUE9,
       RATING_CMP_VALUE10,
       RATING_CMP_VALUE11,
       RATING_CMP_VALUE12,
       RATING_CMP_VALUE13,
       RATING_CMP_VALUE14,
       RATING_CMP_VALUE15,
       RATING_CMP_VALUE16,
       RATING_CMP_VALUE17,
       RATING_CMP_VALUE18,
       RATING_CMP_VALUE19,
       RATING_CMP_VALUE20,
       'N' LOAD_ERROR,
       'S' DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM,
       SYSDATE LASTUPD_EW_DTTM,
       1234 BATCH_SID
  FROM CSSTG_OWNER.PS_STDNT_PKG_VAR V
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_INSTITUTION I
    on V.INSTITUTION = I.INSTITUTION_CD
   and V.SRC_SYS_ID = I.SRC_SYS_ID 
   and I.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_ACAD_CAR C
    on V.ACAD_CAREER = C.ACAD_CAR_CD 
   and V.INSTITUTION = C.INSTITUTION_CD	
   and V.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_PERSON P
    on V.EMPLID = P.PERSON_ID  
   and V.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'   
 where V.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_PKG_VAR rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_PKG_VAR',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_PKG_VAR',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_PKG_VAR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_PKG_VAR enable constraint PK_UM_F_FA_PKG_VAR';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_PKG_VAR');

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

END UM_F_FA_PKG_VAR_P;
/

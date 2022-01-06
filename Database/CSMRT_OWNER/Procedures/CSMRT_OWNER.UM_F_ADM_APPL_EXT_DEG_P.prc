CREATE OR REPLACE PROCEDURE             "UM_F_ADM_APPL_EXT_DEG_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_ADM_APPL_EXT_DEG
--V01 12/12/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_ADM_APPL_EXT_DEG';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_EXT_DEG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_ADM_APPL_EXT_DEG');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_EXT_DEG disable constraint PK_UM_F_ADM_APPL_EXT_DEG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_ADM_APPL_EXT_DEG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_ADM_APPL_EXT_DEG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_ADM_APPL_EXT_DEG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_ADM_APPL_EXT_DEG';				
insert /*+ append */ into UM_F_ADM_APPL_EXT_DEG
   WITH A
        AS (SELECT /*+ INLINE PARALLEL(8) */ DISTINCT APPLCNT_SID,
                            INSTITUTION_SID,
                            SRC_SYS_ID,
                            INSTITUTION_CD
              FROM UM_F_ADM_APPL_STAT),
        O
        AS (SELECT /*+ INLINE PARALLEL(8) */ DISTINCT A.APPLCNT_SID,
                            A.INSTITUTION_SID,
                            NVL (O.EXT_ORG_SID, 2147483646) EXT_ORG_SID,
                            NVL (O.EXT_ACAD_CAR_SID, 2147483646) EXT_ACAD_CAR_SID, -- Added June 2015, Fixed Aug 2015. 
                            NVL (O.EXT_DATA_NBR, 0) EXT_DATA_NBR, -- Added June 2015
                            A.SRC_SYS_ID,
                            A.INSTITUTION_CD,
                            NVL (PERSON_ID, '-') PERSON_ID, -- Added June 2015
                            NVL (EXT_ORG_ID, '-') EXT_ORG_ID, -- Added June 2015
                            NVL (EXT_ACAD_CAR_ID, '-') EXT_ACAD_CAR_ID -- Added June 2015
              FROM A
                   LEFT OUTER JOIN PS_F_EXT_ACAD_SUMM O
                      ON     A.APPLCNT_SID = O.PERSON_SID
                         AND A.INSTITUTION_SID = O.INSTITUTION_SID
                         AND A.SRC_SYS_ID = O.SRC_SYS_ID
                         AND NVL (O.DATA_ORIGIN, '-') <> 'D')
   SELECT /*+ INLINE PARALLEL(8) */ 
          O.APPLCNT_SID,
          O.INSTITUTION_SID,
          O.EXT_ORG_SID,
          O.EXT_ACAD_CAR_SID,                               -- Added June 2015
          O.EXT_DATA_NBR,                                   -- Added June 2015
          NVL(EXT_DEG_NBR, 0),
          O.SRC_SYS_ID,
          O.INSTITUTION_CD,
          O.PERSON_ID EMPLID,
          O.EXT_ORG_ID,
          O.EXT_ACAD_CAR_ID,
          nvl(DESCR,'-'),
          NVL (E.EXT_DEG_SID, 2147483646) EXT_DEG_SID,
          NVL (E.EXT_DATA_SRC_SID, 2147483646) EXT_DATA_SRC_SID,
          NVL (E.EXT_SUBJECT_AREA_SID_1, 2147483646) EXT_SUBJECT_AREA_SID_1,
          NVL (E.EXT_SUBJECT_AREA_SID_2, 2147483646) EXT_SUBJECT_AREA_SID_2,
          nvl(EXT_CAREER,'-'),
          NVL (
             (SELECT MIN (X.XLATSHORTNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE X.FIELDNAME = 'EXT_CAREER' AND X.FIELDVALUE = EXT_CAREER),
             '-')
             EXT_CAREER_SD,
          NVL (
             (SELECT MIN (X.XLATLONGNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE X.FIELDNAME = 'EXT_CAREER' AND X.FIELDVALUE = EXT_CAREER),
             '-')
             EXT_CAREER_LD,
          EXT_DEG_DT,
          nvl(EXT_DEG_STAT_ID,'-'),
          NVL (
             (SELECT MIN (X.XLATSHORTNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'DEGREE_STATUS'
                     AND X.FIELDVALUE = EXT_DEG_STAT_ID),
             '-')
             EXT_DEG_STAT_SD,
          NVL (
             (SELECT MIN (X.XLATLONGNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'DEGREE_STATUS'
                     AND X.FIELDVALUE = EXT_DEG_STAT_ID),
             '-')
             EXT_DEG_STAT_LD,
          nvl(FIELD_OF_STUDY_1,'-'),
          nvl(FIELD_OF_STUDY_2,'-'),
          nvl(HONORS_CATEGORY,'-'),
          NVL (
             (SELECT MIN (X.XLATSHORTNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'HONORS_CATEGORY'
                     AND X.FIELDVALUE = HONORS_CATEGORY),
             '-')
             HONORS_CATEGORY_SD,
          NVL (
             (SELECT MIN (X.XLATLONGNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'HONORS_CATEGORY'
                     AND X.FIELDVALUE = HONORS_CATEGORY),
             '-')
             HONORS_CATEGORY_LD
     FROM O
          LEFT OUTER JOIN UM_F_EXT_DEG E
             ON     O.APPLCNT_SID = E.PERSON_SID
                -- and O.INSTITUTION_SID = E.INSTITUTION_SID
                AND O.EXT_ORG_SID = E.EXT_ORG_SID
                AND O.EXT_ACAD_CAR_ID = E.EXT_CAREER    -- Added June 2015 
                AND O.EXT_DATA_NBR = E.EXT_DATA_NBR    -- Added June 2015 
                AND O.SRC_SYS_ID = E.SRC_SYS_ID
                AND NVL (E.DATA_ORIGIN, '-') <> 'D'
	 where O.APPLCNT_SID <> 2147483646         -- Aug 2018 
       and O.INSTITUTION_SID <> 2147483646     -- Aug 2018 
--       and O.EXT_ORG_SID <> 2147483646     	-- Aug 2018 
--       and O.EXT_ACAD_CAR_SID <> 2147483646    -- Aug 2018 
;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_ADM_APPL_EXT_DEG rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_ADM_APPL_EXT_DEG',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_EXT_DEG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_EXT_DEG enable constraint PK_UM_F_ADM_APPL_EXT_DEG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_ADM_APPL_EXT_DEG');

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

END UM_F_ADM_APPL_EXT_DEG_P;
/

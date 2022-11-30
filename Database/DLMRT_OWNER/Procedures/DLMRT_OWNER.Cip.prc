DROP PROCEDURE DLMRT_OWNER."Cip"
/

--
-- "Cip"  (Procedure) 
--
CREATE OR REPLACE PROCEDURE DLMRT_OWNER."Cip"
        (
                i_MartId                        in  Varchar2    Default 'DLAB',
                i_ProcessName                   in  Varchar2    Default 'Cip'
        )
AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Loads table DLMRT_OWNER.CIP
--
-- V04  SMT-8358 11/12/2019     Greg Kampf
--                              Set UMPO_STEM to Y if the CIP code is in table
--                              CIP_UMPO_STEM.  Otherwise set it to N.
--
-- V03  SMT-8358 10/23/2019     Greg Kampf
--
-- V02  SMT-8358 10/23/2019     Greg Kampf
--
-- V01  SMT-8358 10/23/2019     Greg Kampf
--
------------------------------------------------------------------------

        dtProcessStart                  Date            := SYSDATE;
        intProcessSid                   Integer;
        strMessage01                    Varchar2(32767);
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) := '';
        strSqlDynamic                   Varchar2(32767) := '';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;
        intYear                         Integer;
        strControlRowExists             Varchar2(1);
        intRowNum                       Integer;
        intInsertCount                  Integer         := 0;
        strDataTimestampRowExists       Varchar2(1);

BEGIN
strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (i_ProcessName);

COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => i_MartId,
                i_ProcessName           => i_ProcessName,
                i_ProcessStartTime      => dtProcessStart
        );

strMessage01    := 'Procedure DLMRT_OWNER."Cip" arguments:'
                || strNewLine || '                     i_MartId: ' || i_MartId
                || strNewLine || '                i_ProcessName: ' || i_ProcessName;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic           := 'TRUNCATE TABLE DLMRT_OWNER.CIP';
strSqlCommand           := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (       i_SqlStatement  => strSqlDynamic,
                        i_MaxTries      => 10,
                        i_WaitSeconds   => 10,
                        o_Tries         => intTries
                );

strSqlCommand   := 'SMT_INDEX.ALL_UNUSABLE';
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE
        (
                i_TableOwner                    => 'DLMRT_OWNER',
                i_TableName                     => 'CIP',
                i_IncludeJoinedTables           => True,
                i_IncludePartitionedIndexes     => True,
                i_PartitionName                 => Null,
                i_BitmapsOnly                   => True,
                i_IndexNameNotLike              => 'PK%'
        );

strMessage01 := 'Inserting DLMRT_OWNER.CIP rows...';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
strSqlCommand   := 'INSERT DLMRT_OWNER.CIP';
INSERT  /*+APPEND*/
  INTO  DLMRT_OWNER.CIP
        (
        CIP_CODE,
        CIP_YEAR,
        CIP_DESCR,
        CIP_DESCR254,
        UMPO_STEM,
        CIP_CODE_FAMILY,
        CIP_DESCR_FAMILY,
        CIP_DESCR254_FAMILY,
        CIP_CODE_DISCIPLINE,
        CIP_DESCR_DISCIPLINE,
        CIP_DESCR254_DISCIPLINE,
        INSERT_TIME
        )
WITH CIP_YEARS AS
        (
        SELECT  DISTINCT
                CASE
                  WHEN  TO_NUMBER(TO_CHAR(CIP.EFFDT,'YYYY')) < 2010
                  THEN  2000
                  ELSE  TO_NUMBER(TO_CHAR(CIP.EFFDT,'YYYY'))
                END                                     CIP_YEAR
          FROM  CSSTG_OWNER.PS_CIP_CODE_TBL CIP
         WHERE  CIP.DATA_ORIGIN <> 'D'
        )
SELECT  CIP.CIP_CODE,
        CIP.CIP_YEAR                            CIP_YEAR,
        CIP.DESCR                               CIP_DESCR,
        CIP.DESCR254                            CIP_DESCR254,
        CASE
          WHEN  STM.CIP_CODE IS NULL
          THEN  'N'
          ELSE  'Y'
        END                                     UMPO_STEM,
        FML.CIP_CODE_FAMILY,
        FML.CIP_DESCR_FAMILY,
        FML.CIP_DESCR254_FAMILY,
        DSC.CIP_CODE_DISCIPLINE,
        DSC.CIP_DESCR_DISCIPLINE,
        DSC.CIP_DESCR254_DISCIPLINE,
        SYSDATE                                 INSERT_TIME
  FROM  (
        SELECT  CIP.*,
                CYR.*
          FROM  CIP_YEARS                       CYR,
                CSSTG_OWNER.PS_CIP_CODE_TBL     CIP
         WHERE  CIP.DATA_ORIGIN <> 'D'
           AND  CIP.EFFDT        = (
                                        SELECT  MAX(CP2.EFFDT)
                                          FROM  CSSTG_OWNER.PS_CIP_CODE_TBL CP2
                                         WHERE  CP2.DATA_ORIGIN        <> 'D'
                                           AND  CP2.CIP_CODE            = CIP.CIP_CODE
                                           AND  CP2.SRC_SYS_ID          = CIP.SRC_SYS_ID
                                           AND  CP2.EFFDT              <= TO_DATE('31-DEC-' || TO_CHAR(CYR.CIP_YEAR))
                                   )
           AND  CIP.EFF_STATUS  = 'A'
        )                                       CIP,
        (
        SELECT  CIP.CIP_CODE    CIP_CODE_FAMILY,
                CIP.SRC_SYS_ID,
                CIP.EFFDT,
                CIP.DESCR       CIP_DESCR_FAMILY,
                CIP.DESCR254    CIP_DESCR254_FAMILY,
                CYR.CIP_YEAR
          FROM  CIP_YEARS                       CYR,
                CSSTG_OWNER.PS_CIP_CODE_TBL     CIP
         WHERE  CIP.DATA_ORIGIN     <> 'D'
           AND  LENGTH(CIP.CIP_CODE) = 3
           AND  CIP.EFFDT        = (
                                        SELECT  MAX(CP2.EFFDT)
                                          FROM  CSSTG_OWNER.PS_CIP_CODE_TBL CP2
                                         WHERE  CP2.DATA_ORIGIN        <> 'D'
                                           AND  CP2.CIP_CODE            = CIP.CIP_CODE
                                           AND  CP2.SRC_SYS_ID          = CIP.SRC_SYS_ID
                                           AND  CP2.EFFDT              <= TO_DATE('31-DEC-' || TO_CHAR(CYR.CIP_YEAR))
                                   )
           AND  CIP.EFF_STATUS  = 'A'
        )                                       FML,
        (
        SELECT  CIP.CIP_CODE    CIP_CODE_DISCIPLINE,
                CIP.SRC_SYS_ID,
                CIP.EFFDT,
                CIP.DESCR       CIP_DESCR_DISCIPLINE,
                CIP.DESCR254    CIP_DESCR254_DISCIPLINE,
                CYR.CIP_YEAR
          FROM  CIP_YEARS                       CYR,
                CSSTG_OWNER.PS_CIP_CODE_TBL     CIP
         WHERE  CIP.DATA_ORIGIN     <> 'D'
           AND  LENGTH(CIP.CIP_CODE) = 5
           AND  CIP.EFFDT        = (
                                        SELECT  MAX(CP2.EFFDT)
                                          FROM  CSSTG_OWNER.PS_CIP_CODE_TBL CP2
                                         WHERE  CP2.DATA_ORIGIN        <> 'D'
                                           AND  CP2.CIP_CODE            = CIP.CIP_CODE
                                           AND  CP2.SRC_SYS_ID          = CIP.SRC_SYS_ID
                                           AND  CP2.EFFDT              <= TO_DATE('31-DEC-' || TO_CHAR(CYR.CIP_YEAR))
                                   )
           AND  CIP.EFF_STATUS  = 'A'
        )                                       DSC,
        DLMRT_OWNER.CIP_UMPO_STEM               STM
 WHERE  FML.CIP_CODE_FAMILY(+)          = SUBSTR(CIP.CIP_CODE,1,3)
   AND  FML.SRC_SYS_ID(+)               = CIP.SRC_SYS_ID
   AND  FML.CIP_YEAR(+)                 = CIP.CIP_YEAR
   AND  DSC.CIP_CODE_DISCIPLINE(+)      = SUBSTR(CIP.CIP_CODE,1,5)
   AND  DSC.SRC_SYS_ID(+)               = CIP.SRC_SYS_ID
   AND  DSC.CIP_YEAR(+)                 = CIP.CIP_YEAR
   AND  LENGTH(CIP.CIP_CODE)            = 7
   AND  STM.CIP_CODE(+)                 = CIP.CIP_CODE
;

intRowCount     := SQL%ROWCOUNT;

strSqlCommand   := 'COMMIT';
COMMIT;

-- Insert one row into DLMRT_OWNER.CIP for CIP_YEAR 2010 for a missing Amherst CIP code.  

insert into DLMRT_OWNER.CIP
select CIP_CODE, '2010' CIP_YEAR, CIP_DESCR, CIP_DESCR254, UMPO_STEM, CIP_CODE_FAMILY, CIP_DESCR_FAMILY, CIP_DESCR254_FAMILY, CIP_CODE_DISCIPLINE, CIP_DESCR_DISCIPLINE, CIP_DESCR254_DISCIPLINE, 
       SYSDATE INSERT_TIME
  from DLMRT_OWNER.CIP
 where CIP_CODE = '01.8301'
;

intRowCount     := intRowCount + SQL%ROWCOUNT;

strSqlCommand   := 'COMMIT';
COMMIT;

strMessage01    := '# of rows inserted: ' || TO_CHAR(intRowCount);
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'CIP',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand   := 'SMT_INDEX.ALL_REBUILD';
COMMON_OWNER.SMT_INDEX.ALL_REBUILD
                (
                i_TableOwner            => 'DLMRT_OWNER',
                i_TableName             => 'CIP',
                i_UnusableOnly          => True,
                i_AllJoinedTables       => True,
                i_ParallelDegree        => 1
                );

strSqlCommand   := 'SELECT strDataTimestampRowExists';
SELECT  (
        NVL     (       (
                        SELECT  'Y'
                          FROM  COMMON_OWNER.DATA_TIMESTAMP
                         WHERE  MART_ID         = i_MartId
                           AND  BUSINESS_UNIT   IS NULL
                           AND  COMPONENT       = 'CIP'
                           AND  SOURCE_SYSTEM   = 'FILE'
                        ),
                        'N'
                )
        )       ROW_EXISTS
  INTO  strDataTimestampRowExists
  FROM  DUAL;

If  strDataTimestampRowExists = 'Y'
Then
        strMessage01    := 'Updating COMMON_OWNER.DATA_TIMESTAMP...';
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

        strSqlCommand   := 'UPDATE COMMON_OWNER.DATA_TIMESTAMP';
        UPDATE  COMMON_OWNER.DATA_TIMESTAMP
           SET  SOURCE_TIMESTAMP        = dtProcessStart,
                SOURCE_DATE             = TRUNC(dtProcessStart),
                LAST_UPDATE_TIMESTAMP   = SYSDATE,
                LAST_UPDATE_BY          = i_ProcessName
         WHERE  MART_ID         = i_MartId
           AND  BUSINESS_UNIT   IS NULL
           AND  COMPONENT       = 'CIP'
           AND  SOURCE_SYSTEM   = 'FILE'
        ;

        intRowCount     := SQL%ROWCOUNT;

        strMessage01    := '# of rows updated: ' || TO_CHAR(intRowCount);
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

        strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
                (
                        i_TargetTableName   => 'DATA_TIMESTAMP',
                        i_Action            => 'UPDATE',
                        i_RowCount          => intRowCount
                );
Else
        strMessage01    := 'Inserting COMMON_OWNER.DATA_TIMESTAMP row...';
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

        strSqlCommand   := 'INSERT INTO COMMON_OWNER.DATA_TIMESTAMP';
        INSERT
          INTO  COMMON_OWNER.DATA_TIMESTAMP
                (
                MART_ID,
                BUSINESS_UNIT,
                COMPONENT,
                SOURCE_SYSTEM,
                SOURCE_TIMESTAMP,
                SOURCE_DATE,
                LAST_UPDATE_TIMESTAMP,
                LAST_UPDATE_BY
                )
        SELECT  
                i_MartId                MART_ID,
                TO_CHAR(NULL)           BUSINESS_UNIT,
                'CIP'                   COMPONENT,
                'FILE'                  SOURCE_SYSTEM,
                dtProcessStart          SOURCE_TIMESTAMP,
                TRUNC(dtProcessStart)   SOURCE_DATE,
                SYSDATE                 LAST_UPDATE_TIMESTAMP,
                i_ProcessName           LAST_UPDATE_BY
          FROM  DUAL
        ;

        intRowCount     := SQL%ROWCOUNT;

        strMessage01    := '# of rows inserted: ' || TO_CHAR(intRowCount);
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

        strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
                (
                        i_TargetTableName   => 'DATA_TIMESTAMP',
                        i_Action            => 'INSERT',
                        i_RowCount          => intRowCount
                );
End If;

strSqlCommand   := 'COMMIT';
COMMIT;

strMessage01    := 'Gathering statistics for DLMRT_OWNER.CIP...';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'DBMS_STATS.GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS
        (
                OWNNAME                 => 'DLMRT_OWNER',
                TABNAME                 => 'CIP',
                DEGREE                  => 1,
                ESTIMATE_PERCENT        => DBMS_STATS.AUTO_SAMPLE_SIZE,
                METHOD_OPT              => 'FOR ALL COLUMNS SIZE AUTO',
                GRANULARITY             => 'AUTO',
                FORCE                   => True,
                NO_INVALIDATE           => False
        );

strMessage01    := 'Statistics gathered.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

EXCEPTION
    WHEN OTHERS THEN
        numSqlCode := SQLCODE;
        strSqlErrm := SQLERRM;
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => numSqlCode,
                        i_SqlErrm      => strSqlErrm
                );

END "Cip";
/

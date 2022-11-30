DROP PROCEDURE DLMRT_OWNER."EnrollmentS2"
/

--
-- "EnrollmentS2"  (Procedure) 
--
CREATE OR REPLACE PROCEDURE DLMRT_OWNER."EnrollmentS2"
        (
                i_MartId                in  Varchar2    Default 'DLAB',
                i_ProcessName           in  Varchar2    Default 'EnrollmentS2',
                i_Institution           in  Varchar2
        )
AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Loads stage table DLSTG_OWNER.ENROLLMENT_S2 from table UPLOAD_S1 (UPLOAD_ID 'DLAB_ENROLLMENT_campus')
--
-- V24  CASE-45777      07/29/2020      Jim Doucette
--                                      Added lookup logic to default Census Period to fix bug when recieving records
--                                      for very old terms
-- V23  CASE-30050      05/14/2020      Greg Kampf
--                                      Fix bug that caused failure when the Institution column is null.
-- V22  CASE-30050      05/07/2020      Greg Kampf
--                                      No longer skip row with invalid INSTITUTION/CENSUS_PERIOD/CENSUS_SEQ.
--                                      Report them as errors instead.
--
-- V21  CASE-23700      03/23/2020      Greg Kampf
--                                      Really convert PROGRAM_TYPE from NONDEGREE to Non-Degree.
--
-- V20  CASE-23700      03/19/2020      Greg Kampf
--                                      Support new columns RESIDENCY_CODE_INOUTSTATE, RESIDENCY_DESC_INOUTSTATE
--
-- V19  CASE-23700      03/17/2020      Greg Kampf
--                                      Init cap PROGRAM_TYPE and convert NONDEGREE to Non-Degree.
--
-- V18  CASE-18740      02/18/2020      Greg Kampf
--                                      Improve error information for insert errors.
--
-- V17  SMT-8410 01/21/2020     Greg Kampf
--                              Set new column CENSUS_STATUS.APPROVED_FOR_CONFIRM to N.
--
-- V16  SMT-8358 12/17/2019     Greg Kampf
--                              Treat incomming ACADEMIC_LEVEL_EOT_CODE value of '-' as null.
--
-- V15  SMT-8358 12/12/2019     Greg Kampf
--                              Allow admit census periods not in table CENSUS_PERIOD.
--
-- V14  SMT-8358 12/11/2019     Greg Kampf
--                              Restore missing leading zeroes when looking up zip code on table ZIP_CODE_MA.
--                              Make failed lookup on table ZIP_CODE_MA not an error.
--                              Restore missing leading and trailing zeroes of CIP codes.
--
-- V13  SMT-8358 12/03/2019     Greg Kampf
--                              Change to look up CIP Codes on DLMRT_OWNER.CIP rather than CSSTG_OWNER.PS_CIP_CODE_TBL.
--
-- V12  SMT-8358 12/02/2019     Greg Kampf
--                              Enhancement to add max length to 'value too large' messages
--
-- V11  SMT-8358 11/26/2019     Greg Kampf
--                              Added columns:
--                              o DOCTORATE_TYPE_CODE
--                              o ADMIT_TERM_CODE
--                              o ADMIT_TERM_DESCR
--                              Renamed columns:
--                              o ACADEMIC_LOAD_CODE to FT_PT_CODE
--                              o ACADEMIC_LOAD_DESCR to FT_PT_DESCR
--                              o HYBRID_STUDENT_FLAG to MIXED_MODE_INSTRUCTION_FLAG
--
-- V10  SMT-8358 11/26/2019     Greg Kampf
--                              Use function SMT_INTERFACE.MESSAGE_TRANSLATE to translate specific oracle
--                              messages to somethin more self-service friendly.
--
-- V09  SMT-8358 11/22/2019     Greg Kampf
--                              Deal with files without column headings.
--
-- V08  SMT-8358 11/13/2019     Greg Kampf
--                              Put file name from UPLOAD_S1 into CENSUS_STATUS.
--
-- V07  SMT-8358 11/08/2019     Greg Kampf
--                              If terminated before processing all rows due to too many errors, reject all periods.
--
-- V06  SMT-8358 11/05/2019     Greg Kampf
--                              Support partitioning by INSTITUTION, CENSUS_PERIOD, CENSUS_SEQ.
--
-- V05  SMT-8358 11/04/2019     Greg Kampf
--                              Add update of table CENSUS_STATUS.
--                              This replaces DATA_TIMESTAMP.
--
-- V04  SMT-8358 10/29/2019     Greg Kampf
--                              For non-BDL campuses lookup zip code to get Massachusetts county.
--
-- V03  SMT-8358 10/22/2019     Greg Kampf
--                              More citizenship development.
--
-- V02  SMT-8358 10/17/2019     Greg Kampf
--                              More development.
--
-- V01  SMT-8358 10/10/2019     Greg Kampf
--
------------------------------------------------------------------------

        dtProcessStart                  Date            := SYSDATE;
        intProcessSid                   Integer;
        strMessage01                    Varchar2(32767);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   := '';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) := '';
        strSqlDynamic                   Varchar2(32767) := '';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        intInsertCount                  Integer         := 0;
        intS2InsertCount                Integer;
        intUpdateCount                  Integer;
        strStatusRowExists              Varchar2(1);
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;
        intYear                         Integer;
        strControlRowExists             Varchar2(1);
        strFileName                     Varchar2(4000);
        strPartitionName                Varchar2(128);
        strPartitionValuesList          Varchar2(50);
        strCarriageReturn               Varchar2(1)     := chr(13);
        strQuote                        Varchar2(1)     := chr(39);
        rtpTarget                       DLSTG_OWNER.ENROLLMENT_S2%ROWTYPE; -- Creates a record with columns matching those in the target table
        intErrorCount                   Integer         := 0;
        intRowNum                       Integer;
        intHeaderRowCount               Integer         := 0;
        bolError                        Boolean;
        bolTransformError               Boolean;
        intFailedRowCount               Integer         := 0;
        intFailedRowMax                 Integer         := 100;
        strDataTimestampRowExists       Varchar2(1);
        strCampusId                     Varchar2(3);
        strUploadType                   Varchar2(10)    := 'ENROLLMENT';
        strUploadId                     Varchar2(19);
        strAction                       Varchar2(100);
        intAge                          Integer;
        strMajorSubPlanCipFoundFlag     Varchar2(1);
        strCitizenshipCodeCitizen       Varchar2(1)     := 'C';
        strCitizenshipStatusCitizen     Varchar2(7)     := 'Citizen';
        strCountryCodeUs                Varchar2(2)     := 'US';
        bolPrematureExit                Boolean         := False;
        strPrematureExit                Varchar2(5)     := 'FALSE';
        strTargetColumnName             Varchar2(128);
        strSourceColumnValue            Varchar2(4000);
        intCipYear                      Integer;
        strZipCode                      Varchar2(10);
        strZipCodeNoHyphen              Varchar2(9);
        strYearMin                      Varchar2(4);
        strYearMax                      Varchar2(4);
        strFailureMessage               Varchar2(500);
        strHeading001                   Varchar2(4000)  := 'Column 001';
        strHeading002                   Varchar2(4000)  := 'Column 002';
        strHeading003                   Varchar2(4000)  := 'Column 003';
        strHeading004                   Varchar2(4000)  := 'Column 004';
        strHeading005                   Varchar2(4000)  := 'Column 005';
        strHeading006                   Varchar2(4000)  := 'Column 006';
        strHeading007                   Varchar2(4000)  := 'Column 007';
        strHeading008                   Varchar2(4000)  := 'Column 008';
        strHeading009                   Varchar2(4000)  := 'Column 009';
        strHeading010                   Varchar2(4000)  := 'Column 010';
        strHeading011                   Varchar2(4000)  := 'Column 011';
        strHeading012                   Varchar2(4000)  := 'Column 012';
        strHeading013                   Varchar2(4000)  := 'Column 013';
        strHeading014                   Varchar2(4000)  := 'Column 014';
        strHeading015                   Varchar2(4000)  := 'Column 015';
        strHeading016                   Varchar2(4000)  := 'Column 016';
        strHeading017                   Varchar2(4000)  := 'Column 017';
        strHeading018                   Varchar2(4000)  := 'Column 018';
        strHeading019                   Varchar2(4000)  := 'Column 019';
        strHeading020                   Varchar2(4000)  := 'Column 020';
        strHeading021                   Varchar2(4000)  := 'Column 021';
        strHeading022                   Varchar2(4000)  := 'Column 022';
        strHeading023                   Varchar2(4000)  := 'Column 023';
        strHeading024                   Varchar2(4000)  := 'Column 024';
        strHeading025                   Varchar2(4000)  := 'Column 025';
        strHeading026                   Varchar2(4000)  := 'Column 026';
        strHeading027                   Varchar2(4000)  := 'Column 027';
        strHeading028                   Varchar2(4000)  := 'Column 028';
        strHeading029                   Varchar2(4000)  := 'Column 029';
        strHeading030                   Varchar2(4000)  := 'Column 030';
        strHeading031                   Varchar2(4000)  := 'Column 031';
        strHeading032                   Varchar2(4000)  := 'Column 032';
        strHeading033                   Varchar2(4000)  := 'Column 033';
        strHeading034                   Varchar2(4000)  := 'Column 034';
        strHeading035                   Varchar2(4000)  := 'Column 035';
        strHeading036                   Varchar2(4000)  := 'Column 036';
        strHeading037                   Varchar2(4000)  := 'Column 037';
        strHeading038                   Varchar2(4000)  := 'Column 038';
        strHeading039                   Varchar2(4000)  := 'Column 039';
        strHeading040                   Varchar2(4000)  := 'Column 040';
        strHeading041                   Varchar2(4000)  := 'Column 041';
        strHeading042                   Varchar2(4000)  := 'Column 042';
        strHeading043                   Varchar2(4000)  := 'Column 043';
        strHeading044                   Varchar2(4000)  := 'Column 044';
        strHeading045                   Varchar2(4000)  := 'Column 045';
        strHeading046                   Varchar2(4000)  := 'Column 046';
        strHeading047                   Varchar2(4000)  := 'Column 047';
        strHeading048                   Varchar2(4000)  := 'Column 048';
        strHeading049                   Varchar2(4000)  := 'Column 049';
        strHeading050                   Varchar2(4000)  := 'Column 050';
        strHeading051                   Varchar2(4000)  := 'Column 051';
        strHeading052                   Varchar2(4000)  := 'Column 052';
        strHeading053                   Varchar2(4000)  := 'Column 053';
        strHeading054                   Varchar2(4000)  := 'Column 054';
        strHeading055                   Varchar2(4000)  := 'Column 055';
        strHeading056                   Varchar2(4000)  := 'Column 056';
        strHeading057                   Varchar2(4000)  := 'Column 057';
        strHeading058                   Varchar2(4000)  := 'Column 058';
        strHeading059                   Varchar2(4000)  := 'Column 059';
        strHeading060                   Varchar2(4000)  := 'Column 060';
        strHeading061                   Varchar2(4000)  := 'Column 061';
        strHeading062                   Varchar2(4000)  := 'Column 062';
        strHeading063                   Varchar2(4000)  := 'Column 063';
        strHeading064                   Varchar2(4000)  := 'Column 064';
        strHeading065                   Varchar2(4000)  := 'Column 065';
        strHeading066                   Varchar2(4000)  := 'Column 066';
        strHeading067                   Varchar2(4000)  := 'Column 067';
        strHeading068                   Varchar2(4000)  := 'Column 068';

BEGIN
strSqlCommand   := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (i_ProcessName);

COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => i_MartId,
                i_ProcessName           => i_ProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Procedure DLSTG_OWNER."EnrollmentS2" arguments:'
                || strNewLine || '       i_MartId: ' || i_MartId
                || strNewLine || '  i_ProcessName: ' || i_ProcessName
                || strNewLine || '  i_Institution: ' || i_Institution;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strCampusId     := substr(i_Institution,3,3);
strUploadId     := 'DLAB_ENROLLMENT_' || strCampusId;

-- Get file name
strSqlCommand   := 'SELECT FILE_NAME FROM COMMON_OWNER.UPLOAD_S1';
SELECT  (
        SELECT  MAX(FILE_NAME)  FILE_NAME
          FROM  COMMON_OWNER.UPLOAD_S1  UPL
         WHERE  UPL.UPLOAD_ID           = strUploadId
           AND  UPL.RECORD_NUMBER       = 1
        )                       FILE_NAME
  INTO  strFileName
  FROM  DUAL
;

strSqlCommand   := 'SMT_CONTEXT.SET_ATTRIBUTE';
COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetTableOwner', i_AttributeValue=> 'DLSTG_OWNER');
COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetTableName',  i_AttributeValue=> 'ENROLLMENT_S2');
COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceRowNumber',        i_AttributeValue=> to_char(0));

strSqlCommand := 'SMT_INTERFACE.INTERFACE_INIT';
COMMON_OWNER.SMT_INTERFACE.INTERFACE_INIT
                (       i_SourceTableOwner      => 'COMMON_OWNER',
                        i_SourceTableName       => 'UPLOAD_S1 (UPLOAD_ID: ' || strUploadId || ')',
                        i_TargetTableOwner      => 'DLSTG_OWNER',
                        i_TargetTableName       => 'ENROLLMENT_S2',
                        i_SourceFileName        => strFileName
                );

strPartitionName := 'UPLOAD_' || strUploadId;

strSqlCommand   := 'SMTCMN_PART.LIST_PARTITION_CREATE';
COMMON_OWNER.SMTCMN_PART.LIST_PARTITION_CREATE
        (       i_TableOwner                    => 'DLSTG_OWNER',
                i_TableName                     => 'REJECT_LOG',
                i_PartitionName                 => strPartitionName,
                i_TestMode                      => False,
                i_PartitionValuesList           => strQuote || strUploadId || strQuote
        );

strSqlDynamic   := 'ALTER TABLE DLSTG_OWNER.REJECT_LOG TRUNCATE PARTITION ' || strPartitionName || ' UPDATE INDEXES';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (       i_SqlStatement  => strSqlDynamic,
                        i_MaxTries      => 10,
                        i_WaitSeconds   => 10,
                        o_Tries         => intTries
                );


-- Using DLMRT_OWNER.CENSUS_STATUS identify the institution, census period and census sequences
-- that are more recently loaded in UPLOAD_S1 than in the stage table.
-- For those institution, census period and census sequences combinations truncate their partitions.
strSqlCommand   := 'For recPartition';
For recPartition in
        (
        SELECT  INSTITUTION,
                CENSUS_PERIOD,
                CENSUS_SEQ
          FROM  DLMRT_OWNER.CENSUS_STATUS
         WHERE  UPLOAD_ID               = strUploadId
           AND  UPLOAD_TYPE             = strUploadType
           AND  READY_FOR_STAGE         = 'Y'
        ORDER BY
                INSTITUTION,
                CENSUS_PERIOD,
                CENSUS_SEQ
        )
Loop
        strPartitionName        := recPartition.INSTITUTION || '_' || recPartition.CENSUS_PERIOD || '_' || TO_CHAR(recPartition.CENSUS_SEQ);
        strPartitionValuesList  := strQuote || recPartition.INSTITUTION || strQuote || ',' || strQuote || recPartition.CENSUS_PERIOD || strQuote || ',' || TO_CHAR(recPartition.CENSUS_SEQ);

        strMessage01    := 'Processing partition ' || strPartitionName;
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

        strSqlCommand   := 'SMTCMN_PART.LIST_PARTITION_CREATE';
        COMMON_OWNER.SMTCMN_PART.LIST_PARTITION_CREATE
                (       i_TableOwner                    => 'DLSTG_OWNER',
                        i_TableName                     => 'ENROLLMENT_S2',
                        i_PartitionName                 => strPartitionName,
                        i_TestMode                      => False,
                        i_PartitionValuesList           => strPartitionValuesList
                );

        strSqlDynamic   := 'ALTER TABLE DLSTG_OWNER.ENROLLMENT_S2 TRUNCATE PARTITION "' || strPartitionName || '" UPDATE INDEXES';
        strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
        COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                        i_SqlStatement                  => strSqlDynamic,
                        i_MaxTries                      => 10,
                        i_WaitSeconds                   => 10,
                        o_Tries                         => intTries
                );

        strSqlCommand   := 'SMT_INDEX.ALL_UNUSABLE';
        COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE
                (
                        i_TableOwner                    => 'DLSTG_OWNER',
                        i_TableName                     => 'ENROLLMENT_S2',
                        i_IncludeJoinedTables           => True,
                        i_IncludePartitionedIndexes     => True,
                        i_PartitionName                 => strPartitionName,
                        i_BitmapsOnly                   => True,
                        i_IndexNameNotLike              => 'PK%'
                );
End Loop; -- recPartition

strMessage01 := 'Inserting DLSTG_OWNER.ENROLLMENT_S2 rows from COMMON_OWNER.UPLOAD_S1 (UPLOAD_ID: ' || strUploadId || ')...';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


-- Loop through the source external table which uses the interface file.
strSqlCommand           := 'FOR recUploadS1';
FOR recUploadS1 IN
        (
        SELECT  ROWNUM,
                READY_FOR_STAGE,
                RECORD_NUMBER,
                COLUMN_001,
                COLUMN_002,
                COLUMN_003,
                COLUMN_004,
                COLUMN_005,
                COLUMN_006,
                COLUMN_007,
                COLUMN_008,
                COLUMN_009,
                COLUMN_010,
                COLUMN_011,
                COLUMN_012,
                COLUMN_013,
                COLUMN_014,
                COLUMN_015,
                COLUMN_016,
                COLUMN_017,
                COLUMN_018,
                COLUMN_019,
                COLUMN_020,
                COLUMN_021,
                COLUMN_022,
                COLUMN_023,
                COLUMN_024,
                COLUMN_025,
                COLUMN_026,
                COLUMN_027,
                COLUMN_028,
                COLUMN_029,
                COLUMN_030,
                COLUMN_031,
                COLUMN_032,
                COLUMN_033,
                COLUMN_034,
                COLUMN_035,
                COLUMN_036,
                COLUMN_037,
                COLUMN_038,
                COLUMN_039,
                COLUMN_040,
                COLUMN_041,
                COLUMN_042,
                COLUMN_043,
                COLUMN_044,
                COLUMN_045,
                COLUMN_046,
                COLUMN_047,
                COLUMN_048,
                COLUMN_049,
                COLUMN_050,
                COLUMN_051,
                COLUMN_052,
                COLUMN_053,
                COLUMN_054,
                COLUMN_055,
                COLUMN_056,
                COLUMN_057,
                COLUMN_058,
                COLUMN_059,
                COLUMN_060,
                COLUMN_061,
                COLUMN_062,
                COLUMN_063,
                COLUMN_064,
                COLUMN_065,
                COLUMN_066,
                COLUMN_067,
                COLUMN_068
          FROM  (
                SELECT  CST.READY_FOR_STAGE,
                        UPL.*
                  FROM  COMMON_OWNER.UPLOAD_S1          UPL,
                        (
                        SELECT
                                CS1.UPLOAD_TYPE,
                                CS1.UPLOAD_ID,
                                CS1.INSTITUTION,
                                CS1.CENSUS_PERIOD,
                                TRIM(TO_CHAR(CS1.CENSUS_SEQ))   CENSUS_SEQ,
                                CS1.READY_FOR_STAGE
                          FROM  DLMRT_OWNER.CENSUS_STATUS CS1
                        )                               CST
                 WHERE  UPL.UPLOAD_ID                           = strUploadId
                   AND  UPL.RECORD_NUMBER                       > 1
                   AND  CST.UPLOAD_TYPE                 (+)     = strUploadType
                   AND  CST.UPLOAD_ID                   (+)     = UPL.UPLOAD_ID
                   AND  CST.INSTITUTION                 (+)     = trim(UPL.COLUMN_001)
                   AND  CST.CENSUS_PERIOD               (+)     = trim(UPL.COLUMN_002)
                   AND  CST.CENSUS_SEQ                  (+)     = trim(UPL.COLUMN_003)
                   AND  CST.READY_FOR_STAGE             (+)     = 'Y'
                UNION ALL
                SELECT  Null                    READY_FOR_STAGE,
                        UPL.*
                  FROM  COMMON_OWNER.UPLOAD_S1  UPL
                 WHERE  UPL.UPLOAD_ID           = strUploadId
                   AND  UPL.RECORD_NUMBER       = 1
                )
        ORDER BY
                RECORD_NUMBER
        )
Loop
--    If  recUploadS1.COLUMN_001 = 'Inst'
    If  recUploadS1.RECORD_NUMBER = 1
    --and recUploadS1.COLUMN_001 <> i_Institution
    Then
        intHeaderRowCount       := intHeaderRowCount + 1;

        -- Get column headings from the header row.
        strHeading001   := recUploadS1.COLUMN_001;
        strHeading002   := recUploadS1.COLUMN_002;
        strHeading003   := recUploadS1.COLUMN_003;
        strHeading004   := recUploadS1.COLUMN_004;
        strHeading005   := recUploadS1.COLUMN_005;
        strHeading006   := recUploadS1.COLUMN_006;
        strHeading007   := recUploadS1.COLUMN_007;
        strHeading008   := recUploadS1.COLUMN_008;
        strHeading009   := recUploadS1.COLUMN_009;
        strHeading010   := recUploadS1.COLUMN_010;
        strHeading011   := recUploadS1.COLUMN_011;
        strHeading012   := recUploadS1.COLUMN_012;
        strHeading013   := recUploadS1.COLUMN_013;
        strHeading014   := recUploadS1.COLUMN_014;
        strHeading015   := recUploadS1.COLUMN_015;
        strHeading016   := recUploadS1.COLUMN_016;
        strHeading017   := recUploadS1.COLUMN_017;
        strHeading018   := recUploadS1.COLUMN_018;
        strHeading019   := recUploadS1.COLUMN_019;
        strHeading020   := recUploadS1.COLUMN_020;
        strHeading021   := recUploadS1.COLUMN_021;
        strHeading022   := recUploadS1.COLUMN_022;
        strHeading023   := recUploadS1.COLUMN_023;
        strHeading024   := recUploadS1.COLUMN_024;
        strHeading025   := recUploadS1.COLUMN_025;
        strHeading026   := recUploadS1.COLUMN_026;
        strHeading027   := recUploadS1.COLUMN_027;
        strHeading028   := recUploadS1.COLUMN_028;
        strHeading029   := recUploadS1.COLUMN_029;
        strHeading030   := recUploadS1.COLUMN_030;
        strHeading031   := recUploadS1.COLUMN_031;
        strHeading032   := recUploadS1.COLUMN_032;
        strHeading033   := recUploadS1.COLUMN_033;
        strHeading034   := recUploadS1.COLUMN_034;
        strHeading035   := recUploadS1.COLUMN_035;
        strHeading036   := recUploadS1.COLUMN_036;
        strHeading037   := recUploadS1.COLUMN_037;
        strHeading038   := recUploadS1.COLUMN_038;
        strHeading039   := recUploadS1.COLUMN_039;
        strHeading040   := recUploadS1.COLUMN_040;
        strHeading041   := recUploadS1.COLUMN_041;
        strHeading042   := recUploadS1.COLUMN_042;
        strHeading043   := recUploadS1.COLUMN_043;
        strHeading044   := recUploadS1.COLUMN_044;
        strHeading045   := recUploadS1.COLUMN_045;
        strHeading046   := recUploadS1.COLUMN_046;
        strHeading047   := recUploadS1.COLUMN_047;
        strHeading048   := recUploadS1.COLUMN_048;
        strHeading049   := recUploadS1.COLUMN_049;
        strHeading050   := recUploadS1.COLUMN_050;
        strHeading051   := recUploadS1.COLUMN_051;
        strHeading052   := recUploadS1.COLUMN_052;
        strHeading053   := recUploadS1.COLUMN_053;
        strHeading054   := recUploadS1.COLUMN_054;
        strHeading055   := recUploadS1.COLUMN_055;
        strHeading056   := recUploadS1.COLUMN_056;
        strHeading057   := recUploadS1.COLUMN_057;
        strHeading058   := recUploadS1.COLUMN_058;
        strHeading059   := recUploadS1.COLUMN_059;
        strHeading060   := recUploadS1.COLUMN_060;
        strHeading061   := recUploadS1.COLUMN_061;
        strHeading062   := recUploadS1.COLUMN_062;
        strHeading063   := recUploadS1.COLUMN_063;
        strHeading064   := recUploadS1.COLUMN_064;
        strHeading065   := recUploadS1.COLUMN_065;
        strHeading066   := recUploadS1.COLUMN_066;
        strHeading067   := recUploadS1.COLUMN_067;
        strHeading068   := recUploadS1.COLUMN_068;
    Else
        rtpTarget.PROCESS_SID := Null;
        rtpTarget.PROCESS_START_DATE := Null;
        rtpTarget.RECORD_NUMBER := Null;
        rtpTarget.INSTITUTION := Null;
        rtpTarget.CAMPUS := Null;
        rtpTarget.CENSUS_PERIOD := Null;
        rtpTarget.CENSUS_PERIOD_DESCR := Null;
        rtpTarget.CENSUS_SEQ := Null;
        rtpTarget.PERSON_ID := Null;
        rtpTarget.UMASS_GUID := Null;
        rtpTarget.BIRTH_DATE := Null;
        rtpTarget.CAMPUS_CENSUS_DATE := Null;
        rtpTarget.IPEDS_AGE_RANGE_CODE := Null;
        rtpTarget.IPEDS_AGE_RANGE_DESCR := Null;
        rtpTarget.GENDER_CODE := Null;
        rtpTarget.GENDER_DESCR := Null;
        rtpTarget.US_CITIZENSHIP_CODE := Null;
        rtpTarget.US_CITIZENSHIP_STATUS := Null;
        rtpTarget.COUNTRY_2CHAR_NON_US := Null;
        rtpTarget.COUNTRY_DESCR_NON_US := Null;
        rtpTarget.FED_ETHNICITY_CODE := Null;
        rtpTarget.FED_ETHNICITY_DESCR := Null;
        rtpTarget.STATE_ETHNICITY_CODE := Null;
        rtpTarget.STATE_ETHNICITY_DESCR := Null;
        rtpTarget.UNDER_REPRESENTED_MINORITY := Null;
        rtpTarget.MILITARY_STATUS := Null;
        rtpTarget.COUNTRY_2CHAR_PERM_ADDRESS := Null;
        rtpTarget.COUNTRY_DESCR_PERM_ADDRESS := Null;
        rtpTarget.STATE_PERM_ADDRESS := Null;
        rtpTarget.POSTAL_CODE_PERM_ADDRESS := Null;
        rtpTarget.COUNTY_PERM_ADDRESS := Null;
        rtpTarget.CAMPUS_RESIDENT := Null;
        rtpTarget.RESIDENCY_CODE_SA_TUITION := Null;
        rtpTarget.RESIDENCY_DESC_SA_TUITION := Null;
        rtpTarget.RESIDENCY_CODE_TUITION := Null;
        rtpTarget.RESIDENCY_DESC_TUITION := Null;
        rtpTarget.RESIDENCY_CODE_INOUTSTATE := Null;
        rtpTarget.RESIDENCY_DESC_INOUTSTATE := Null;
        rtpTarget.COUNTRY_2CHAR_NON_US_1 := Null;
        rtpTarget.COUNTRY_DESCR_NON_US_1 := Null;
        rtpTarget.CITIZENSHIP_CODE_NON_US_1 := Null;
        rtpTarget.CITIZENSHIP_STATUS_NON_US_1 := Null;
        rtpTarget.COUNTRY_2CHAR_NON_US_2 := Null;
        rtpTarget.COUNTRY_DESCR_NON_US_2 := Null;
        rtpTarget.CITIZENSHIP_CODE_NON_US_2 := Null;
        rtpTarget.CITIZENSHIP_STATUS_NON_US_2 := Null;
        rtpTarget.COUNTRY_2CHAR_NON_US_3 := Null;
        rtpTarget.COUNTRY_DESCR_NON_US_3 := Null;
        rtpTarget.CITIZENSHIP_CODE_NON_US_3 := Null;
        rtpTarget.CITIZENSHIP_STATUS_NON_US_3 := Null;
        rtpTarget.ACADEMIC_CAREER_PS := Null;
        rtpTarget.STUDENT_CAREER_NUMBER_PS := Null;
        rtpTarget.TERM_CODE_PS := Null;
        rtpTarget.HEADCOUNT := Null;
        rtpTarget.CAREER_LEVEL_CODE := Null;
        rtpTarget.PROGRAM_TYPE := Null;
        rtpTarget.EDUCATION_LEVEL_CODE := Null;
        rtpTarget.EDUCATION_LEVEL_DESCR := Null;
        rtpTarget.DOCTORATE_TYPE_CODE := Null;
        rtpTarget.CES_STUDENT_FLAG := Null;
        rtpTarget.REPORTED_TO_IPEDS_FLAG := Null;
        rtpTarget.ADMIT_TYPE := Null;
        rtpTarget.ADMIT_TERM_CODE := Null;
        rtpTarget.ADMIT_TERM_DESCR := Null;
        rtpTarget.FIRST_GENERATION_FLAG := Null;
        rtpTarget.NEW_OR_CONTINUING := Null;
        rtpTarget.PRIMARY_COLLEGE_CODE := Null;
        rtpTarget.PRIMARY_COLLEGE_DESCR := Null;
        rtpTarget.PRIMARY_MAJOR_CODE := Null;
        rtpTarget.PRIMARY_MAJOR_DESCR := Null;
        rtpTarget.PRIMARY_MAJOR_CIP_CODE := Null;
        rtpTarget.PRIMARY_MAJOR_PLAN_TYPE := Null;
        rtpTarget.STEM_FLAG := Null;
        rtpTarget.PRIMARY_MAJOR_SUB_PLAN_TYPE := Null;
        rtpTarget.PRIMARY_MAJOR_SUB_PLAN_CODE := Null;
        rtpTarget.PRIMARY_MAJOR_SUB_PLAN_DESCR := Null;
        rtpTarget.PRIMARY_MAJOR_SUB_PLAN_CIP_CODE := Null;
        rtpTarget.ACADEMIC_LEVEL_BOT_CODE := Null;
        rtpTarget.ACADEMIC_LEVEL_BOT_DESCR := Null;
        rtpTarget.ACADEMIC_LEVEL_EOT_CODE := Null;
        rtpTarget.ACADEMIC_LEVEL_EOT_DESCR := Null;
        rtpTarget.EXPECTED_GRAD_TERM_CODE := Null;
        rtpTarget.EXPECTED_GRAD_TERM_DESCR := Null;
        rtpTarget.CUMULATIVE_CREDITS := Null;
        rtpTarget.CUMULATIVE_GPA := Null;
        rtpTarget.FT_PT_CODE := Null;
        rtpTarget.FT_PT_DESCR := Null;
        rtpTarget.TOTAL_CREDITS := Null;
        rtpTarget.ONLINE_CREDITS := Null;
        rtpTarget.NON_ONLINE_CREDITS := Null;
        rtpTarget.ONLINE_ONLY_STUDENT_FLAG := Null;
        rtpTarget.MIXED_MODE_INSTRUCTION_FLAG := Null;
        rtpTarget.CE_CREDITS := Null;
        rtpTarget.NON_CE_CREDITS := Null;
        rtpTarget.TOTAL_FTE := Null;
        rtpTarget.ONLINE_FTE := Null;
        rtpTarget.CE_FTE := Null;
        rtpTarget.CLASS_COUNT := Null;
        rtpTarget.ONLINE_CLASS_COUNT := Null;
        rtpTarget.CE_CLASS_COUNT := Null;
        rtpTarget.ALL_REGISTRATION_COUNT := Null;
        rtpTarget.ONLINE_REGISTRATION_COUNT := Null;
        rtpTarget.CE_REGISTRATION_COUNT := Null;
        rtpTarget.PELL_RECIPIENT_FLAG := Null;
        rtpTarget.INSERT_TIME := Null;

        bolError        := False;
        intRowNum       := recUploadS1.RECORD_NUMBER;

        strSqlCommand   := 'SMT_CONTEXT.SET_ATTRIBUTE';
        COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceRowNumber', i_AttributeValue=> to_char(intRowNum));


--      Move each source row's unformatted varchar2 column value to the corresponding column of
--      the record that matches the target table's columns doing appropriate transformation.
--      This allows individual trapping and reporting of transformation errors.
--      If there is an error SMT_INTERFACE.LOG_ERROR is called to record it and the process continues
--      to the next column.
        Begin
                strAction               := 'transformation';
                strTargetColumnName     := 'INSTITUTION';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_001);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.INSTITUTION   := strSourceColumnValue;
                If  rtpTarget.INSTITUTION <> i_Institution
                Then
                        strAction       := 'validation';
                        strSqlCommand   := 'Column validation';
                        RAISE_APPLICATION_ERROR( -20001, 'Value of source column ' || strHeading001 || ' must be ' || i_Institution);
                End If;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolTransformError
                                        := True;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column ' || strAction || ' error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading001,
                                        i_SourceColumnName      => 'COLUMN_001'
                                );
        End;
        Begin
                strTargetColumnName     := 'CENSUS_PERIOD';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_002);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.CENSUS_PERIOD := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading002,
                                        i_SourceColumnName      => 'COLUMN_002'
                                );
        End;
        Begin
                strTargetColumnName     := 'CENSUS_SEQ';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_003);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.CENSUS_SEQ    := to_number(strSourceColumnValue);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading003,
                                        i_SourceColumnName      => 'COLUMN_003'
                                );
        End;
        If  recUploadS1.READY_FOR_STAGE is Null
        or  recUploadS1.READY_FOR_STAGE <> 'Y'
        Then
                strTargetColumnName     := 'INSTITUTION/CENSUS_PERIOD/CENSUS_SEQ';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_001) || '/' || trim(recUploadS1.COLUMN_002) || '/' || trim(recUploadS1.COLUMN_003);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                intErrorCount   := intErrorCount + 1;
                bolError        := True;
                COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                        (
                                i_ErrorSequence         => intErrorCount,
                                i_ErrorDescription      => 'Invalid record',
                                i_ErrorMessage          => 'Invalid census: ' || strSourceColumnValue,
                                i_ErrorCode             => Null,
                                i_SourceColumnHeading   => '', --strHeading001 || '/' || strHeading002 || '/' || strHeading003,
                                i_SourceColumnName      => 'COLUMN_001/002/003'
                        );
        End If;
        Begin
                strTargetColumnName     := 'PERSON_ID';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_004);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.PERSON_ID     := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading004,
                                        i_SourceColumnName      => 'COLUMN_004'
                                );
        End;
        Begin
                strTargetColumnName     := 'UMASS_GUID';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_005);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.UMASS_GUID    := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading005,
                                        i_SourceColumnName      => 'COLUMN_005'
                                );
        End;
        Begin
                strTargetColumnName     := 'BIRTH_DATE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_006);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.BIRTH_DATE    := to_date(strSourceColumnValue,'MM/DD/YYYY');
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading006,
                                        i_SourceColumnName      => 'COLUMN_006'
                                );
        End;
        Begin
                strTargetColumnName     := 'CAMPUS_CENSUS_DATE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_007);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.CAMPUS_CENSUS_DATE    := to_date(strSourceColumnValue,'MM/DD/YYYY');
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading007,
                                        i_SourceColumnName      => 'COLUMN_007'
                                );
        End;

--        intCipYear      := trunc(to_number(to_char(rtpTarget.CAMPUS_CENSUS_DATE,'YYYY')),-1);

        Begin
                strTargetColumnName     := 'GENDER_CODE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_008);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.GENDER_CODE   := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading008,
                                        i_SourceColumnName      => 'COLUMN_008'
                                );
        End;
        Begin
                strTargetColumnName     := 'US_CITIZENSHIP_CODE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_009);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.US_CITIZENSHIP_CODE   := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading009,
                                        i_SourceColumnName      => 'COLUMN_009'
                                );
        End;
        Begin
                strTargetColumnName     := 'COUNTRY_2CHAR_NON_US';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_010);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.COUNTRY_2CHAR_NON_US  := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading010,
                                        i_SourceColumnName      => 'COLUMN_010'
                                );
        End;
        rtpTarget.COUNTRY_2CHAR_NON_US_1  := rtpTarget.COUNTRY_2CHAR_NON_US;
        Begin
                strTargetColumnName     := 'COUNTRY_2CHAR_NON_US_2';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_011);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.COUNTRY_2CHAR_NON_US_2  := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading011,
                                        i_SourceColumnName      => 'COLUMN_011'
                                );
        End;
        Begin
                strTargetColumnName     := 'COUNTRY_2CHAR_NON_US_3';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_012);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.COUNTRY_2CHAR_NON_US_3  := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading012,
                                        i_SourceColumnName      => 'COLUMN_012'
                                );
        End;
        Begin
                strTargetColumnName     := 'FED_ETHNICITY_CODE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_013);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.FED_ETHNICITY_CODE    := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading013,
                                        i_SourceColumnName      => 'COLUMN_013'
                                );
        End;
        Begin
                strTargetColumnName     := 'STATE_ETHNICITY_CODE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_014);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.STATE_ETHNICITY_CODE    := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading014,
                                        i_SourceColumnName      => 'COLUMN_014'
                                );
        End;
        Begin
                strTargetColumnName     := 'UNDER_REPRESENTED_MINORITY';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_015);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.UNDER_REPRESENTED_MINORITY    := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading015,
                                        i_SourceColumnName      => 'COLUMN_015'
                                );
        End;
        Begin
                strTargetColumnName     := 'MILITARY_STATUS';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_016);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.MILITARY_STATUS       := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading016,
                                        i_SourceColumnName      => 'COLUMN_016'
                                );
        End;
        Begin
                strTargetColumnName     := 'COUNTRY_2CHAR_PERM_ADDRESS';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_017);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.COUNTRY_2CHAR_PERM_ADDRESS    := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading017,
                                        i_SourceColumnName      => 'COLUMN_017'
                                );
        End;
        Begin
                strTargetColumnName     := 'STATE_PERM_ADDRESS';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_018);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.STATE_PERM_ADDRESS    := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading018,
                                        i_SourceColumnName      => 'COLUMN_018'
                                );
        End;
        Begin
                strTargetColumnName     := 'POSTAL_CODE_PERM_ADDRESS';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_019);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                If  rtpTarget.COUNTRY_2CHAR_PERM_ADDRESS = 'US'
                Then
                        strZipCodeNoHyphen      := Case
                                                     When  Length(strSourceColumnValue) <= 5
                                                     Then  trim(to_char(to_number(strSourceColumnValue),'00000'))
                                                     Else  trim(to_char(to_number(replace(strSourceColumnValue,'-','')),'000000000'))
                                                   End;
                        If  length(strZipCodeNoHyphen) = 9
                        Then
                                strZipCode      := substr(strZipCodeNoHyphen,1,5) || '-' || substr(strZipCodeNoHyphen,6,4);
                        Else
                                strZipCode      := strZipCodeNoHyphen;
                        End If;
                        rtpTarget.POSTAL_CODE_PERM_ADDRESS      := strZipCode;
                Else
                        rtpTarget.POSTAL_CODE_PERM_ADDRESS      := strSourceColumnValue;
                End If;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading019,
                                        i_SourceColumnName      => 'COLUMN_019'
                                );
        End;
        Begin
                strTargetColumnName     := 'COUNTY_PERM_ADDRESS';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_020);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.COUNTY_PERM_ADDRESS    := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading020,
                                        i_SourceColumnName      => 'COLUMN_020'
                                );
        End;
        Begin
                strTargetColumnName     := 'CAMPUS_RESIDENT';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_021);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.CAMPUS_RESIDENT    := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading021,
                                        i_SourceColumnName      => 'COLUMN_021'
                                );
        End;
        Begin
                strTargetColumnName     := 'RESIDENCY_CODE_SA_TUITION';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_022);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.RESIDENCY_CODE_SA_TUITION     := Case
                                                                When    strSourceColumnValue = '-'
                                                                Then    ''
                                                                Else    trim(to_char(to_number(strSourceColumnValue),'00'))
                                                           End;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading022,
                                        i_SourceColumnName      => 'COLUMN_022'
                                );
        End;

        Begin
                strTargetColumnName     := 'ACADEMIC_CAREER_PS';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_023);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.ACADEMIC_CAREER_PS    := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading023,
                                        i_SourceColumnName      => 'COLUMN_023'
                                );
        End;

        Begin
                strTargetColumnName     := 'STUDENT_CAREER_NUMBER_PS';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_024);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.STUDENT_CAREER_NUMBER_PS    := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading024,
                                        i_SourceColumnName      => 'COLUMN_024'
                                );
        End;

        Begin
                strTargetColumnName     := 'TERM_CODE_PS';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_025);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.TERM_CODE_PS    := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading025,
                                        i_SourceColumnName      => 'COLUMN_025'
                                );
        End;

        rtpTarget.HEADCOUNT     := 1;

        Begin
                strTargetColumnName     := 'CAREER_LEVEL_CODE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_026);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.CAREER_LEVEL_CODE     := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading026,
                                        i_SourceColumnName      => 'COLUMN_026'
                                );
        End;

        Begin
                strTargetColumnName     := 'PROGRAM_TYPE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_027);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.PROGRAM_TYPE  := Case
                                                When    UPPER(strSourceColumnValue) = 'NONDEGREE'
                                                Then    'Non-Degree'
                                                Else    INITCAP(strSourceColumnValue)
                                           End;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading027,
                                        i_SourceColumnName      => 'COLUMN_027'
                                );
        End;

        Begin
                strTargetColumnName     := 'EDUCATION_LEVEL_CODE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_028);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.EDUCATION_LEVEL_CODE  := trim(TO_CHAR(TO_NUMBER(strSourceColumnValue),'00'));
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading028,
                                        i_SourceColumnName      => 'COLUMN_028'
                                );
        End;

        Begin
                strTargetColumnName     := 'DOCTORATE_TYPE_CODE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_029);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.DOCTORATE_TYPE_CODE  := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading029,
                                        i_SourceColumnName      => 'COLUMN_029'
                                );
        End;

        Begin
                strTargetColumnName     := 'CES_STUDENT_FLAG';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_030);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.CES_STUDENT_FLAG  := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading030,
                                        i_SourceColumnName      => 'COLUMN_030'
                                );
        End;

        Begin
                strTargetColumnName     := 'REPORTED_TO_IPEDS_FLAG';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_031);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.REPORTED_TO_IPEDS_FLAG  := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading031,
                                        i_SourceColumnName      => 'COLUMN_031'
                                );
        End;

        Begin
                strTargetColumnName     := 'ADMIT_TYPE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_032);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.ADMIT_TYPE    := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading032,
                                        i_SourceColumnName      => 'COLUMN_032'
                                );
        End;

        Begin
                strTargetColumnName     := 'ADMIT_TERM_CODE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_033);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.ADMIT_TERM_CODE := to_number(strSourceColumnValue); -- triggers error if not numeric
                rtpTarget.ADMIT_TERM_CODE := strSourceColumnValue; -- triggers error if too long
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading033,
                                        i_SourceColumnName      => 'COLUMN_033'
                                );
        End;

        Begin
                strTargetColumnName     := 'FIRST_GENERATION_FLAG';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_034);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.FIRST_GENERATION_FLAG  := NVL(strSourceColumnValue,'U');
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading034,
                                        i_SourceColumnName      => 'COLUMN_034'
                                );
        End;

        Begin
                strTargetColumnName     := 'NEW_OR_CONTINUING';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_035);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.NEW_OR_CONTINUING  := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading035,
                                        i_SourceColumnName      => 'COLUMN_035'
                                );
        End;

        Begin
                strTargetColumnName     := 'PRIMARY_COLLEGE_CODE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_036);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.PRIMARY_COLLEGE_CODE  := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading036,
                                        i_SourceColumnName      => 'COLUMN_036'
                                );
        End;

        Begin
                strTargetColumnName     := 'PRIMARY_COLLEGE_DESCR';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_037);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.PRIMARY_COLLEGE_DESCR  := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading037,
                                        i_SourceColumnName      => 'COLUMN_037'
                                );
        End;

        Begin
                strTargetColumnName     := 'PRIMARY_MAJOR_CODE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_038);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.PRIMARY_MAJOR_CODE  := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading038,
                                        i_SourceColumnName      => 'COLUMN_038'
                                );
        End;

        Begin
                strTargetColumnName     := 'PRIMARY_MAJOR_DESCR';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_039);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.PRIMARY_MAJOR_DESCR  := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading039,
                                        i_SourceColumnName      => 'COLUMN_039'
                                );
        End;

        Begin
                strTargetColumnName     := 'PRIMARY_MAJOR_CIP_CODE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_040);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.PRIMARY_MAJOR_CIP_CODE  := Case
                                                        When    strSourceColumnValue = '-'
                                                        Then    Null
                                                        Else    trim(to_char(to_number(strSourceColumnValue),'00.0000'))
                                                      End;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading040,
                                        i_SourceColumnName      => 'COLUMN_040'
                                );
        End;

        Begin
                strTargetColumnName     := 'PRIMARY_MAJOR_PLAN_TYPE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_041);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.PRIMARY_MAJOR_PLAN_TYPE  := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading041,
                                        i_SourceColumnName      => 'COLUMN_041'
                                );
        End;

        Begin
                strTargetColumnName     := 'PRIMARY_MAJOR_SUB_PLAN_TYPE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_042);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.PRIMARY_MAJOR_SUB_PLAN_TYPE  := Case strSourceColumnValue
                                                                When    '-'
                                                                Then    Null
                                                                Else    strSourceColumnValue
                                                          End;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading042,
                                        i_SourceColumnName      => 'COLUMN_042'
                                );
        End;

        Begin
                strTargetColumnName     := 'PRIMARY_MAJOR_SUB_PLAN_CODE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_043);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.PRIMARY_MAJOR_SUB_PLAN_CODE  := Case strSourceColumnValue
                                                                When    '-'
                                                                Then    Null
                                                                Else    strSourceColumnValue
                                                          End;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading043,
                                        i_SourceColumnName      => 'COLUMN_043'
                                );
        End;


        Begin
                strTargetColumnName     := 'PRIMARY_MAJOR_SUB_PLAN_DESCR';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_044);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.PRIMARY_MAJOR_SUB_PLAN_DESCR  := Case strSourceColumnValue
                                                                When    '-'
                                                                Then    Null
                                                                Else    strSourceColumnValue
                                                           End;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading044,
                                        i_SourceColumnName      => 'COLUMN_044'
                                );
        End;

        Begin
                strTargetColumnName     := 'PRIMARY_MAJOR_SUB_PLAN_CIP_CODE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_045);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.PRIMARY_MAJOR_SUB_PLAN_CIP_CODE := trim(to_char(to_number(strSourceColumnValue),'00.0000'));
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading045,
                                        i_SourceColumnName      => 'COLUMN_045'
                                );
        End;

        Begin
                strTargetColumnName     := 'ACADEMIC_LEVEL_BOT_CODE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_046);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.ACADEMIC_LEVEL_BOT_CODE       := Case
                                                                When    strSourceColumnValue = '-'
                                                                Then    ''
                                                                Else    strSourceColumnValue
                                                           End;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading046,
                                        i_SourceColumnName      => 'COLUMN_046'
                                );
        End;

        Begin
                strTargetColumnName     := 'ACADEMIC_LEVEL_EOT_CODE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_047);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.ACADEMIC_LEVEL_EOT_CODE       := Case
                                                                When    strSourceColumnValue = '-'
                                                                Then    ''
                                                                Else    strSourceColumnValue
                                                           End;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading047,
                                        i_SourceColumnName      => 'COLUMN_047'
                                );
        End;

        Begin
                strTargetColumnName     := 'EXPECTED_GRAD_TERM_CODE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_048);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.EXPECTED_GRAD_TERM_CODE       := Case
                                                                When    strSourceColumnValue = '-'
                                                                Then    ''
                                                                Else    strSourceColumnValue
                                                           End;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading048,
                                        i_SourceColumnName      => 'COLUMN_048'
                                );
        End;

        Begin
                strTargetColumnName     := 'CUMULATIVE_CREDITS';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_049);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.CUMULATIVE_CREDITS := TO_NUMBER(strSourceColumnValue);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading049,
                                        i_SourceColumnName      => 'COLUMN_049'
                                );
        End;

        Begin
                strTargetColumnName     := 'CUMULATIVE_GPA';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_050);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.CUMULATIVE_GPA := TO_NUMBER(strSourceColumnValue);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading050,
                                        i_SourceColumnName      => 'COLUMN_050'
                                );
        End;

        Begin
                strTargetColumnName     := 'FT_PT_CODE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_051);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.FT_PT_CODE    := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading051,
                                        i_SourceColumnName      => 'COLUMN_051'
                                );
        End;

        Begin
                strTargetColumnName     := 'TOTAL_CREDITS';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_052);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.TOTAL_CREDITS := TO_NUMBER(strSourceColumnValue);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading052,
                                        i_SourceColumnName      => 'COLUMN_052'
                                );
        End;

        Begin
                strTargetColumnName     := 'ONLINE_CREDITS';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_053);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.ONLINE_CREDITS := TO_NUMBER(strSourceColumnValue);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading053,
                                        i_SourceColumnName      => 'COLUMN_053'
                                );
        End;

        rtpTarget.NON_ONLINE_CREDITS := rtpTarget.TOTAL_CREDITS - rtpTarget.ONLINE_CREDITS;

        Begin
                strTargetColumnName     := 'ONLINE_ONLY_STUDENT_FLAG';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_055);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.ONLINE_ONLY_STUDENT_FLAG := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading055,
                                        i_SourceColumnName      => 'COLUMN_055'
                                );
        End;

        Begin
                strTargetColumnName     := 'MIXED_MODE_INSTRUCTION_FLAG';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_056);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.MIXED_MODE_INSTRUCTION_FLAG := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading056,
                                        i_SourceColumnName      => 'COLUMN_056'
                                );
        End;

        Begin
                strTargetColumnName     := 'CE_CREDITS';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_057);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.CE_CREDITS := TO_NUMBER(strSourceColumnValue);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading057,
                                        i_SourceColumnName      => 'COLUMN_057'
                                );
        End;

        Begin
                strTargetColumnName     := 'NON_CE_CREDITS';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_058);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.NON_CE_CREDITS := TO_NUMBER(strSourceColumnValue);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading058,
                                        i_SourceColumnName      => 'COLUMN_058'
                                );
        End;

        Begin
                strTargetColumnName     := 'TOTAL_FTE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_059);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.TOTAL_FTE := TO_NUMBER(strSourceColumnValue);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading059,
                                        i_SourceColumnName      => 'COLUMN_059'
                                );
        End;

        Begin
                strTargetColumnName     := 'ONLINE_FTE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_060);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.ONLINE_FTE := TO_NUMBER(strSourceColumnValue);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading060,
                                        i_SourceColumnName      => 'COLUMN_060'
                                );
        End;

        Begin
                strTargetColumnName     := 'CE_FTE';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_061);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.CE_FTE := TO_NUMBER(strSourceColumnValue);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading061,
                                        i_SourceColumnName      => 'COLUMN_061'
                                );
        End;

        Begin
                strTargetColumnName     := 'CLASS_COUNT';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_062);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.CLASS_COUNT := TO_NUMBER(strSourceColumnValue);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading062,
                                        i_SourceColumnName      => 'COLUMN_062'
                                );
        End;

        Begin
                strTargetColumnName     := 'ONLINE_CLASS_COUNT';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_063);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.ONLINE_CLASS_COUNT := TO_NUMBER(strSourceColumnValue);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading063,
                                        i_SourceColumnName      => 'COLUMN_063'
                                );
        End;

        Begin
                strTargetColumnName     := 'CE_CLASS_COUNT';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_064);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.CE_CLASS_COUNT := TO_NUMBER(strSourceColumnValue);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading064,
                                        i_SourceColumnName      => 'COLUMN_064'
                                );
        End;

        Begin
                strTargetColumnName     := 'ALL_REGISTRATION_COUNT';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_065);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.ALL_REGISTRATION_COUNT := TO_NUMBER(strSourceColumnValue);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading065,
                                        i_SourceColumnName      => 'COLUMN_065'
                                );
        End;

        Begin
                strTargetColumnName     := 'ONLINE_REGISTRATION_COUNT';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_066);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.ONLINE_REGISTRATION_COUNT := TO_NUMBER(strSourceColumnValue);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading066,
                                        i_SourceColumnName      => 'COLUMN_066'
                                );
        End;

        Begin
                strTargetColumnName     := 'CE_REGISTRATION_COUNT';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_067);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.CE_REGISTRATION_COUNT := TO_NUMBER(strSourceColumnValue);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading067,
                                        i_SourceColumnName      => 'COLUMN_067'
                                );
        End;

        Begin
                strTargetColumnName     := 'PELL_RECIPIENT_FLAG';
                strSourceColumnValue    := trim(recUploadS1.COLUMN_068);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> strTargetColumnName);
                COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> strSourceColumnValue);
                rtpTarget.PELL_RECIPIENT_FLAG := strSourceColumnValue;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                        i_ErrorCode             => SQLCODE,
                                        i_SourceColumnHeading   => strHeading068,
                                        i_SourceColumnName      => 'COLUMN_068'
                                );
        End;

        COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceTargetColumnName',  i_AttributeValue=> Null);
        COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'SourceColumnValue',          i_AttributeValue=> Null);

--      Lookup type transformations
        strSqlCommand   := 'Lookup Campus';
        strAction       := 'translation lookup';
        SELECT  NVL(    (
                        SELECT  TRN.DESCRIPTION
                          FROM  DLSTG_OWNER.TRANSLATION TRN
                         WHERE  TRN.DATA_NAME            = 'INSTITUTION'
                           AND  TRN.CODE                 = rtpTarget.INSTITUTION
                           AND  TRN.EFFECTIVE_TERM      <= rtpTarget.CENSUS_PERIOD
                           AND  TRN.EFFECTIVE_END_TERM  >= rtpTarget.CENSUS_PERIOD
                        ),
                        'Lookup failed'
                   )                    DESCRIPTION
          INTO  rtpTarget.CAMPUS
          FROM  DUAL;
        If  rtpTarget.CAMPUS = 'Lookup failed'
        Then
                intErrorCount   := intErrorCount + 1;
                bolTransformError
                                := True;
                bolError        := True;
                strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                        (
                                i_ErrorSequence         => intErrorCount,
                                i_ErrorDescription      => 'Column ' || strAction || ' error',
                                i_ErrorMessage          => 'Error looking up campus name for institution',
                                i_ErrorCode             => Null,
                                i_SourceColumnHeading   => strHeading001,
                                i_SourceColumnName      => 'COLUMN_001',
                                i_TargetColumnName      => 'CAMPUS',
                                i_ColumnValue           => recUploadS1.COLUMN_001
                        );
        End If;

        strSqlCommand   := 'Lookup CENSUS_PERIOD';
        strAction       := 'translation lookup';
/*		Case-45777 July 2020
        SELECT  NVL(    (
                        SELECT  TRN.TERM_DESCRIPTION
                          FROM  DLMRT_OWNER.CENSUS_PERIOD TRN
                         WHERE  TRN.CENSUS_PERIOD       = rtpTarget.CENSUS_PERIOD
                        ),
                        'Lookup failed'
                   )                    TERM_DESCRIPTION
          INTO  rtpTarget.CENSUS_PERIOD_DESCR
          FROM  DUAL;
*/

--		SELECT NVL(  (                                       --Case-45777 July 2020
--				SELECT TRN.TERM_DESCRIPTION
--				 FROM DLMRT_OWNER.CENSUS_PERIOD TRN
--				 WHERE TRN.CENSUS_PERIOD    = (case when rtpTarget.CENSUS_PERIOD < '20002'
--									 and length(rtpTarget.CENSUS_PERIOD) = 5
--									then '00000'
--									else rtpTarget.CENSUS_PERIOD
--								  end)
--				),
--				'Lookup failed'
--			  )          TERM_DESCRIPTION
--		 INTO rtpTarget.CENSUS_PERIOD_DESCR
--		 FROM DUAL;

        with P1 as (
				SELECT TRN.TERM_DESCRIPTION, TRN.CIP_YEAR
				 FROM DLMRT_OWNER.CENSUS_PERIOD TRN
				 WHERE TRN.CENSUS_PERIOD    = (case when rtpTarget.CENSUS_PERIOD < '20002'
									 and length(rtpTarget.CENSUS_PERIOD) = 5
									then '00000'
									else rtpTarget.CENSUS_PERIOD
								  end))
        select  nvl((select TERM_DESCRIPTION from P1),'Lookup failed') TERM_DESCRIPTION,
                nvl((select CIP_YEAR from P1),0) CIP_YEAR
		  into  rtpTarget.CENSUS_PERIOD_DESCR,
		        intCipYear
          from  DUAL;

        If  rtpTarget.CENSUS_PERIOD_DESCR = 'Lookup failed'
        Then
                intErrorCount   := intErrorCount + 1;
                bolTransformError
                                := True;
                bolError        := True;
                strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                        (
                                i_ErrorSequence         => intErrorCount,
                                i_ErrorDescription      => 'Column ' || strAction || ' error',
                                i_ErrorMessage          => 'Error looking up description for census period',
                                i_ErrorCode             => Null,
                                i_SourceColumnHeading   => strHeading002,
                                i_SourceColumnName      => 'COLUMN_002',
                                i_TargetColumnName      => 'CENSUS_PERIOD_DESCR',
                                i_ColumnValue           => recUploadS1.COLUMN_002
                        );
        End If;

        strSqlCommand   := 'Determine age';
        intAge          := trunc(months_between(rtpTarget.CAMPUS_CENSUS_DATE, rtpTarget.BIRTH_DATE)/12);
        strSqlCommand   := 'Determine age code';
        rtpTarget.IPEDS_AGE_RANGE_CODE
                        := Case
                                When    intAge    IS Null
                                Then    '99'
                                When    intAge    < 18
                                Then    '01'
                                When    intAge    < 20
                                Then    '02'
                                When    intAge    < 22
                                Then    '03'
                                When    intAge    < 25
                                Then    '04'
                                When    intAge    < 30
                                Then    '05'
                                When    intAge    < 35
                                Then    '06'
                                When    intAge    < 40
                                Then    '07'
                                When    intAge    < 50
                                Then    '08'
                                When    intAge    < 65
                                Then    '09'
                                Else    '10'
                           End;

        strSqlCommand   := 'Get IPEDS_AGE_RANGE_DESCR';
        strAction       := 'translation lookup';
        SELECT  NVL(    (
                        SELECT  TRN.DESCRIPTION
                          FROM  DLSTG_OWNER.TRANSLATION TRN
                         WHERE  TRN.DATA_NAME            = 'IPEDS_AGE_RANGE'
                           AND  TRN.CODE                 = rtpTarget.IPEDS_AGE_RANGE_CODE
                           AND  TRN.EFFECTIVE_TERM      <= rtpTarget.CENSUS_PERIOD
                           AND  TRN.EFFECTIVE_END_TERM  >= rtpTarget.CENSUS_PERIOD
                        ),
                        'Lookup failed'
                   )                    DESCRIPTION
          INTO  rtpTarget.IPEDS_AGE_RANGE_DESCR
          FROM  DUAL;
        If  rtpTarget.IPEDS_AGE_RANGE_DESCR = 'Lookup failed'
        Then
                intErrorCount   := intErrorCount + 1;
                bolTransformError
                                := True;
                bolError        := True;
                strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                        (
                                i_ErrorSequence         => intErrorCount,
                                i_ErrorDescription      => 'Column ' || strAction || ' error',
                                i_ErrorMessage          => 'Error looking up description for IPEDS age range code',
                                i_ErrorCode             => Null,
                                i_SourceColumnHeading   => strHeading006,
                                i_SourceColumnName      => 'COLUMN_006',
                                i_TargetColumnName      => 'IPEDS_AGE_RANGE_DESCR',
                                i_ColumnValue           => recUploadS1.COLUMN_006
                        );
        End If;

        strSqlCommand   := 'Get GENDER_DESCR';
        strAction       := 'translation lookup';
        SELECT  NVL(    (
                        SELECT  TRN.DESCRIPTION
                          FROM  DLSTG_OWNER.TRANSLATION TRN
                         WHERE  TRN.DATA_NAME            = 'GENDER'
                           AND  TRN.CODE                 = rtpTarget.GENDER_CODE
                           AND  TRN.EFFECTIVE_TERM      <= rtpTarget.CENSUS_PERIOD
                           AND  TRN.EFFECTIVE_END_TERM  >= rtpTarget.CENSUS_PERIOD
                        ),
                        'Lookup failed'
                   )                    DESCRIPTION
          INTO  rtpTarget.GENDER_DESCR
          FROM  DUAL;
        If  rtpTarget.GENDER_DESCR = 'Lookup failed'
        Then
                intErrorCount   := intErrorCount + 1;
                bolTransformError
                                := True;
                bolError        := True;
                strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                        (
                                i_ErrorSequence         => intErrorCount,
                                i_ErrorDescription      => 'Column ' || strAction || ' error',
                                i_ErrorMessage          => 'Error looking up description for gender code',
                                i_ErrorCode             => Null,
                                i_SourceColumnHeading   => strHeading008,
                                i_SourceColumnName      => 'COLUMN_008',
                                i_TargetColumnName      => 'GENDER_DESCR',
                                i_ColumnValue           => recUploadS1.COLUMN_008
                        );
        End If;

        strSqlCommand   := 'Get US_CITIZENSHIP_STATUS';
        strAction       := 'translation lookup';
        SELECT  NVL(    (
                        SELECT  TRN.DESCRIPTION
                          FROM  DLSTG_OWNER.TRANSLATION TRN
                         WHERE  TRN.DATA_NAME            = 'US_CITIZENSHIP'
                           AND  TRN.CODE                 = rtpTarget.US_CITIZENSHIP_CODE
                           AND  TRN.EFFECTIVE_TERM      <= rtpTarget.CENSUS_PERIOD
                           AND  TRN.EFFECTIVE_END_TERM  >= rtpTarget.CENSUS_PERIOD
                        ),
                        'Lookup failed'
                   )                    DESCRIPTION
          INTO  rtpTarget.US_CITIZENSHIP_STATUS
          FROM  DUAL;
        If  rtpTarget.US_CITIZENSHIP_STATUS = 'Lookup failed'
        Then
                intErrorCount   := intErrorCount + 1;
                bolTransformError
                                := True;
                bolError        := True;
                strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                        (
                                i_ErrorSequence         => intErrorCount,
                                i_ErrorDescription      => 'Column ' || strAction || ' error',
                                i_ErrorMessage          => 'Error looking up description for US citizenship code',
                                i_ErrorCode             => Null,
                                i_SourceColumnHeading   => strHeading009,
                                i_SourceColumnName      => 'COLUMN_009',
                                i_TargetColumnName      => 'US_CITIZENSHIP_STATUS',
                                i_ColumnValue           => recUploadS1.COLUMN_009
                        );
        End If;

        If  /*rtpTarget.US_CITIZENSHIP_CODE in ('C', 'P')
        and */rtpTarget.COUNTRY_2CHAR_NON_US is Null
        Then
                rtpTarget.COUNTRY_DESCR_NON_US := '';
        Else
                strSqlCommand   := 'Get COUNTRY_DESCR_NON_US';
                strAction       := 'translation lookup';
                SELECT  NVL(    (
                                SELECT  CNT.COUNTRY_DESCRIPTION
                                  FROM  DLMRT_OWNER.COUNTRY CNT
                                 WHERE  CNT.COUNTRY_2CHAR       = rtpTarget.COUNTRY_2CHAR_NON_US
                                ),
                                'Lookup failed'
                           )                    COUNTRY_DESCRIPTION
                  INTO  rtpTarget.COUNTRY_DESCR_NON_US
                  FROM  DUAL;
                If  rtpTarget.COUNTRY_DESCR_NON_US = 'Lookup failed'
                Then
                        intErrorCount   := intErrorCount + 1;
                        bolTransformError
                                        := True;
                        bolError        := True;
                        strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column ' || strAction || ' error',
                                        i_ErrorMessage          => 'Error looking up description for non-US country code',
                                        i_ErrorCode             => Null,
                                        i_SourceColumnHeading   => strHeading010,
                                        i_SourceColumnName      => 'COLUMN_010',
                                        i_TargetColumnName      => 'COUNTRY_DESCR_NON_US',
                                        i_ColumnValue           => recUploadS1.COLUMN_010
                                );
                End If;
        End If;

        If  rtpTarget.COUNTRY_2CHAR_NON_US_1 = strCountryCodeUs
        Then
                intErrorCount   := intErrorCount + 1;
                bolTransformError
                                := True;
                bolError        := True;
                strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                        (
                                i_ErrorSequence         => intErrorCount,
                                i_ErrorDescription      => 'Column validation error',
                                i_ErrorMessage          => 'Value must not be "US"',
                                i_ErrorCode             => Null,
                                i_SourceColumnHeading   => strHeading010,
                                i_SourceColumnName      => 'COLUMN_010',
                                i_TargetColumnName      => 'COUNTRY_DESCR_NON_US_1',
                                i_ColumnValue           => recUploadS1.COLUMN_010
                        );
        End If;

        If  rtpTarget.COUNTRY_2CHAR_NON_US_2 = strCountryCodeUs
        Then
                intErrorCount   := intErrorCount + 1;
                bolTransformError
                                := True;
                bolError        := True;
                strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                        (
                                i_ErrorSequence         => intErrorCount,
                                i_ErrorDescription      => 'Column validation error',
                                i_ErrorMessage          => 'Value must not be "US"',
                                i_ErrorCode             => Null,
                                i_SourceColumnHeading   => strHeading011,
                                i_SourceColumnName      => 'COLUMN_011',
                                i_TargetColumnName      => 'COUNTRY_DESCR_NON_US_2',
                                i_ColumnValue           => recUploadS1.COLUMN_011
                        );
        End If;

        If  rtpTarget.COUNTRY_2CHAR_NON_US_3 = strCountryCodeUs
        Then
                intErrorCount   := intErrorCount + 1;
                bolTransformError
                                := True;
                bolError        := True;
                strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                        (
                                i_ErrorSequence         => intErrorCount,
                                i_ErrorDescription      => 'Column validation error',
                                i_ErrorMessage          => 'Value must not be "US"',
                                i_ErrorCode             => Null,
                                i_SourceColumnHeading   => strHeading012,
                                i_SourceColumnName      => 'COLUMN_012',
                                i_TargetColumnName      => 'COUNTRY_DESCR_NON_US_3',
                                i_ColumnValue           => recUploadS1.COLUMN_012
                        );
        End If;


        If  rtpTarget.COUNTRY_2CHAR_NON_US_1 is Null
        Then
                rtpTarget.COUNTRY_DESCR_NON_US_1        := '';
                rtpTarget.CITIZENSHIP_CODE_NON_US_1     := '';
                rtpTarget.CITIZENSHIP_STATUS_NON_US_1   := '';
        Else
                rtpTarget.COUNTRY_DESCR_NON_US_1        := rtpTarget.COUNTRY_DESCR_NON_US;
                rtpTarget.CITIZENSHIP_CODE_NON_US_1     := strCitizenshipCodeCitizen;
                rtpTarget.CITIZENSHIP_STATUS_NON_US_1   := strCitizenshipStatusCitizen;
/*
                strSqlCommand   := 'Get COUNTRY_DESCR_NON_US_1';
                strAction       := 'translation lookup';
                SELECT  NVL(    (
                                SELECT  CNT.COUNTRY_DESCRIPTION
                                  FROM  DLMRT_OWNER.COUNTRY CNT
                                 WHERE  CNT.COUNTRY_2CHAR       = rtpTarget.COUNTRY_2CHAR_NON_US_1
                                ),
                                'Lookup failed'
                           )                    COUNTRY_DESCRIPTION
                  INTO  rtpTarget.COUNTRY_DESCR_NON_US_1
                  FROM  DUAL;
                If  rtpTarget.COUNTRY_DESCR_NON_US = 'Lookup failed'
                Then
                        intErrorCount   := intErrorCount + 1;
                        bolTransformError
                                        := True;
                        bolError        := True;
                        strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column ' || strAction || ' error',
                                        i_ErrorMessage          => 'Error looking up description for non-US country code 1 "' || rtpTarget.COUNTRY_2CHAR_NON_US_1 || '"',
                                        i_ErrorCode             => Null,
                                        i_SourceColumnHeading   => strHeading010,
                                        i_SourceColumnName      => 'COLUMN_010',
                                        i_TargetColumnName      => 'COUNTRY_DESCR_NON_US_1',
                                        i_ColumnValue           => recUploadS1.COLUMN_010
                                );
                Else
                        rtpTarget.CITIZENSHIP_CODE_NON_US_1     := strCitizenshipCodeCitizen;
                        rtpTarget.CITIZENSHIP_STATUS_NON_US_1   := strCitizenshipStatusCitizen;
                End If;
*/
        End If;

        If  rtpTarget.COUNTRY_2CHAR_NON_US_2 is Null
        Then
                rtpTarget.COUNTRY_DESCR_NON_US_2        := '';
                rtpTarget.CITIZENSHIP_CODE_NON_US_2     := '';
                rtpTarget.CITIZENSHIP_STATUS_NON_US_2   := '';
        Else
                strSqlCommand   := 'Get COUNTRY_DESCR_NON_US_2';
                strAction       := 'translation lookup';
                SELECT  NVL(    (
                                SELECT  CNT.COUNTRY_DESCRIPTION
                                  FROM  DLMRT_OWNER.COUNTRY CNT
                                 WHERE  CNT.COUNTRY_2CHAR       = rtpTarget.COUNTRY_2CHAR_NON_US_2
                                ),
                                'Lookup failed'
                           )                    COUNTRY_DESCRIPTION
                  INTO  rtpTarget.COUNTRY_DESCR_NON_US_2
                  FROM  DUAL;
                If  rtpTarget.COUNTRY_DESCR_NON_US = 'Lookup failed'
                Then
                        intErrorCount   := intErrorCount + 1;
                        bolTransformError
                                        := True;
                        bolError        := True;
                        strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column ' || strAction || ' error',
                                        i_ErrorMessage          => 'Error looking up description for non-US country code 2',
                                        i_ErrorCode             => Null,
                                        i_SourceColumnHeading   => strHeading011,
                                        i_SourceColumnName      => 'COLUMN_011',
                                        i_TargetColumnName      => 'COUNTRY_DESCR_NON_US_2',
                                        i_ColumnValue           => recUploadS1.COLUMN_011
                                );
                Else
                        rtpTarget.CITIZENSHIP_CODE_NON_US_2     := strCitizenshipCodeCitizen;
                        rtpTarget.CITIZENSHIP_STATUS_NON_US_2   := strCitizenshipStatusCitizen;
                End If;
        End If;

        If  rtpTarget.COUNTRY_2CHAR_NON_US_3 is Null
        Then
                rtpTarget.COUNTRY_DESCR_NON_US_3        := '';
                rtpTarget.CITIZENSHIP_CODE_NON_US_3     := '';
                rtpTarget.CITIZENSHIP_STATUS_NON_US_3   := '';
        Else
                strSqlCommand   := 'Get COUNTRY_DESCR_NON_US_3';
                strAction       := 'translation lookup';
                SELECT  NVL(    (
                                SELECT  CNT.COUNTRY_DESCRIPTION
                                  FROM  DLMRT_OWNER.COUNTRY CNT
                                 WHERE  CNT.COUNTRY_2CHAR       = rtpTarget.COUNTRY_2CHAR_NON_US_3
                                ),
                                'Lookup failed'
                           )                    COUNTRY_DESCRIPTION
                  INTO  rtpTarget.COUNTRY_DESCR_NON_US_3
                  FROM  DUAL;
                If  rtpTarget.COUNTRY_DESCR_NON_US = 'Lookup failed'
                Then
                        intErrorCount   := intErrorCount + 1;
                        bolTransformError
                                        := True;
                        bolError        := True;
                        strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column ' || strAction || ' error',
                                        i_ErrorMessage          => 'Error looking up description for non-US country code 3',
                                        i_ErrorCode             => Null,
                                        i_SourceColumnHeading   => strHeading012,
                                        i_SourceColumnName      => 'COLUMN_012',
                                        i_TargetColumnName      => 'COUNTRY_DESCR_NON_US_3',
                                        i_ColumnValue           => recUploadS1.COLUMN_012
                                );
                Else
                        rtpTarget.CITIZENSHIP_CODE_NON_US_3     := strCitizenshipCodeCitizen;
                        rtpTarget.CITIZENSHIP_STATUS_NON_US_3   := strCitizenshipStatusCitizen;
                End If;
        End If;

        If  rtpTarget.COUNTRY_2CHAR_NON_US_1 = strCountryCodeUs
        Then
                intErrorCount   := intErrorCount + 1;
                bolTransformError
                                := True;
                bolError        := True;
                strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                        (
                                i_ErrorSequence         => intErrorCount,
                                i_ErrorDescription      => 'Column validation error',
                                i_ErrorMessage          => 'Value must not be "US"',
                                i_ErrorCode             => Null,
                                i_SourceColumnHeading   => strHeading010,
                                i_SourceColumnName      => 'COLUMN_010',
                                i_TargetColumnName      => 'COUNTRY_DESCR_NON_US_1',
                                i_ColumnValue           => recUploadS1.COLUMN_010
                        );
        End If;

        If  rtpTarget.COUNTRY_2CHAR_NON_US_2 = rtpTarget.COUNTRY_2CHAR_NON_US_1
        Then
                intErrorCount   := intErrorCount + 1;
                bolTransformError
                                := True;
                bolError        := True;
                strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                        (
                                i_ErrorSequence         => intErrorCount,
                                i_ErrorDescription      => 'Column validation error',
                                i_ErrorMessage          => 'Value must not equal that of column ' || strHeading010,
                                i_ErrorCode             => Null,
                                i_SourceColumnHeading   => strHeading011,
                                i_SourceColumnName      => 'COLUMN_011',
                                i_TargetColumnName      => 'COUNTRY_DESCR_NON_US_2',
                                i_ColumnValue           => recUploadS1.COLUMN_011
                        );
        End If;

        If  rtpTarget.COUNTRY_2CHAR_NON_US_3 = rtpTarget.COUNTRY_2CHAR_NON_US_1
        or  rtpTarget.COUNTRY_2CHAR_NON_US_3 = rtpTarget.COUNTRY_2CHAR_NON_US_2
        Then
                intErrorCount   := intErrorCount + 1;
                bolTransformError
                                := True;
                bolError        := True;
                strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                        (
                                i_ErrorSequence         => intErrorCount,
                                i_ErrorDescription      => 'Column validation error',
                                i_ErrorMessage          => 'Value must not equal that of columns ' || strHeading010 || ' or ' || strHeading010,
                                i_ErrorCode             => Null,
                                i_SourceColumnHeading   => strHeading012,
                                i_SourceColumnName      => 'COLUMN_012',
                                i_TargetColumnName      => 'COUNTRY_DESCR_NON_US_3',
                                i_ColumnValue           => recUploadS1.COLUMN_012
                        );
        End If;

        strSqlCommand   := 'Get FED_ETHNICITY_DESCR';
        strAction       := 'translation lookup';
        SELECT  NVL(    (
                        SELECT  TRN.DESCRIPTION
                          FROM  DLSTG_OWNER.TRANSLATION TRN
                         WHERE  TRN.DATA_NAME            = 'FEDERAL_ETHNICITY'
                           AND  TRN.CODE                 = rtpTarget.FED_ETHNICITY_CODE
                           AND  TRN.EFFECTIVE_TERM      <= rtpTarget.CENSUS_PERIOD
                           AND  TRN.EFFECTIVE_END_TERM  >= rtpTarget.CENSUS_PERIOD
                        ),
                        'Lookup failed'
                   )                    DESCRIPTION
          INTO  rtpTarget.FED_ETHNICITY_DESCR
          FROM  DUAL;
        If  rtpTarget.FED_ETHNICITY_DESCR = 'Lookup failed'
        Then
                intErrorCount   := intErrorCount + 1;
                bolTransformError
                                := True;
                bolError        := True;
                strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                        (
                                i_ErrorSequence         => intErrorCount,
                                i_ErrorDescription      => 'Column ' || strAction || ' error',
                                i_ErrorMessage          => 'Error looking up description for federal ethnicity code',
                                i_ErrorCode             => Null,
                                i_SourceColumnHeading   => strHeading013,
                                i_SourceColumnName      => 'COLUMN_013',
                                i_TargetColumnName      => 'FED_ETHNICITY_DESCR',
                                i_ColumnValue           => recUploadS1.COLUMN_013
                        );
        End If;

        strSqlCommand   := 'Get STATE_ETHNICITY_DESCR';
        strAction       := 'translation lookup';
        SELECT  NVL(    (
                        SELECT  TRN.DESCRIPTION
                          FROM  DLSTG_OWNER.TRANSLATION TRN
                         WHERE  TRN.DATA_NAME            = 'STATE_ETHNICITY'
                           AND  TRN.CODE                 = rtpTarget.STATE_ETHNICITY_CODE
                           AND  TRN.EFFECTIVE_TERM      <= rtpTarget.CENSUS_PERIOD
                           AND  TRN.EFFECTIVE_END_TERM  >= rtpTarget.CENSUS_PERIOD
                        ),
                        'Lookup failed'
                   )                    DESCRIPTION
          INTO  rtpTarget.STATE_ETHNICITY_DESCR
          FROM  DUAL;
        If  rtpTarget.STATE_ETHNICITY_DESCR = 'Lookup failed'
        Then
                intErrorCount   := intErrorCount + 1;
                bolTransformError
                                := True;
                bolError        := True;
                strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                        (
                                i_ErrorSequence         => intErrorCount,
                                i_ErrorDescription      => 'Column ' || strAction || ' error',
                                i_ErrorMessage          => 'Error looking up description for state ethnicity code',
                                i_ErrorCode             => Null,
                                i_SourceColumnHeading   => strHeading014,
                                i_SourceColumnName      => 'COLUMN_014',
                                i_TargetColumnName      => 'STATE_ETHNICITY_DESCR',
                                i_ColumnValue           => recUploadS1.COLUMN_014
                        );
        End If;

        If  rtpTarget.COUNTRY_2CHAR_PERM_ADDRESS is Null
        Then
                rtpTarget.COUNTRY_2CHAR_PERM_ADDRESS := '';
        Else
                strSqlCommand   := 'Get COUNTRY_DESCR_PERM_ADDRESS';
                strAction       := 'translation lookup';
                SELECT  NVL(    (
                                SELECT  CNT.COUNTRY_DESCRIPTION
                                  FROM  DLMRT_OWNER.COUNTRY CNT
                                 WHERE  CNT.COUNTRY_2CHAR       = rtpTarget.COUNTRY_2CHAR_PERM_ADDRESS
                                ),
                                'Lookup failed'
                           )                    COUNTRY_DESCRIPTION
                  INTO  rtpTarget.COUNTRY_DESCR_PERM_ADDRESS
                  FROM  DUAL;
                If  rtpTarget.COUNTRY_DESCR_PERM_ADDRESS = 'Lookup failed'
                Then
                        intErrorCount   := intErrorCount + 1;
                        bolTransformError
                                        := True;
                        bolError        := True;
                        strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column ' || strAction || ' error',
                                        i_ErrorMessage          => 'Error looking up description for permanent country code',
                                        i_ErrorCode             => Null,
                                        i_SourceColumnHeading   => strHeading017,
                                        i_SourceColumnName      => 'COLUMN_017',
                                        i_TargetColumnName      => 'COUNTRY_DESCR_PERM_ADDRESS',
                                        i_ColumnValue           => recUploadS1.COLUMN_017
                                );
                End If;
        End If;

        strSqlCommand   := '';

        rtpTarget.RESIDENCY_CODE_TUITION        := Case
                                                        When    rtpTarget.RESIDENCY_CODE_SA_TUITION in ('04', '05')
                                                        Then    '02'
                                                        Else    rtpTarget.RESIDENCY_CODE_SA_TUITION
                                                   End;

        rtpTarget.RESIDENCY_CODE_INOUTSTATE     := Case
                                                        When    rtpTarget.RESIDENCY_CODE_SA_TUITION in ('03', '04', '05')
                                                        Then    '02'
                                                        Else    rtpTarget.RESIDENCY_CODE_SA_TUITION
                                                   End;

        If  rtpTarget.RESIDENCY_CODE_SA_TUITION is Null
        Then
                rtpTarget.RESIDENCY_DESC_SA_TUITION := '';
        Else
                strSqlCommand   := 'Get RESIDENCY_DESC_SA_TUITION';
                strAction       := 'translation lookup';
                SELECT  NVL(    (
                                SELECT  TRN.DESCRIPTION
                                  FROM  DLSTG_OWNER.TRANSLATION TRN
                                 WHERE  TRN.DATA_NAME            = 'SA_TUITION_RESIDENCY'
                                   AND  TRN.CODE                 = rtpTarget.RESIDENCY_CODE_SA_TUITION
                                   AND  TRN.EFFECTIVE_TERM      <= rtpTarget.CENSUS_PERIOD
                                   AND  TRN.EFFECTIVE_END_TERM  >= rtpTarget.CENSUS_PERIOD
                                ),
                                'Lookup failed'
                           )                    DESCRIPTION
                  INTO  rtpTarget.RESIDENCY_DESC_SA_TUITION
                  FROM  DUAL;
                If  rtpTarget.RESIDENCY_DESC_SA_TUITION = 'Lookup failed'
                Then
                        intErrorCount   := intErrorCount + 1;
                        bolTransformError
                                        := True;
                        bolError        := True;
                        strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column ' || strAction || ' error',
                                        i_ErrorMessage          => 'Error looking up description for SA tuition residency code',
                                        i_ErrorCode             => Null,
                                        i_SourceColumnHeading   => strHeading022,
                                        i_SourceColumnName      => 'COLUMN_022',
                                        i_TargetColumnName      => 'RESIDENCY_DESC_SA_TUITION',
                                        i_ColumnValue           => recUploadS1.COLUMN_022
                                );
                End If;
        End If;

        If  rtpTarget.RESIDENCY_CODE_TUITION is Null
        Then
                rtpTarget.RESIDENCY_DESC_TUITION := '';
        Else
                strSqlCommand   := 'Get RESIDENCY_DESC_TUITION';
                strAction       := 'translation lookup';
                SELECT  NVL(    (
                                SELECT  TRN.DESCRIPTION
                                  FROM  DLSTG_OWNER.TRANSLATION TRN
                                 WHERE  TRN.DATA_NAME            = 'SA_TUITION_RESIDENCY'
                                   AND  TRN.CODE                 = rtpTarget.RESIDENCY_CODE_TUITION
                                   AND  TRN.EFFECTIVE_TERM      <= rtpTarget.CENSUS_PERIOD
                                   AND  TRN.EFFECTIVE_END_TERM  >= rtpTarget.CENSUS_PERIOD
                                ),
                                'Lookup failed'
                           )                    DESCRIPTION
                  INTO  rtpTarget.RESIDENCY_DESC_TUITION
                  FROM  DUAL;
                If  rtpTarget.RESIDENCY_DESC_TUITION = 'Lookup failed'
                Then
                        intErrorCount   := intErrorCount + 1;
                        bolTransformError
                                        := True;
                        bolError        := True;
                        strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column ' || strAction || ' error',
                                        i_ErrorMessage          => 'Error looking up description for tuition residency code',
                                        i_ErrorCode             => Null,
                                        i_SourceColumnHeading   => strHeading022,
                                        i_SourceColumnName      => 'COLUMN_022',
                                        i_TargetColumnName      => 'RESIDENCY_DESC_TUITION',
                                        i_ColumnValue           => recUploadS1.COLUMN_022
                                );
                End If;
        End If;

        If  rtpTarget.RESIDENCY_CODE_INOUTSTATE is Null
        Then
                rtpTarget.RESIDENCY_DESC_INOUTSTATE := '';
        Else
                strSqlCommand   := 'Get RESIDENCY_DESC_INOUTSTATE';
                strAction       := 'translation lookup';
                SELECT  NVL(    (
                                SELECT  TRN.DESCRIPTION
                                  FROM  DLSTG_OWNER.TRANSLATION TRN
                                 WHERE  TRN.DATA_NAME            = 'SA_TUITION_RESIDENCY'
                                   AND  TRN.CODE                 = rtpTarget.RESIDENCY_CODE_INOUTSTATE
                                   AND  TRN.EFFECTIVE_TERM      <= rtpTarget.CENSUS_PERIOD
                                   AND  TRN.EFFECTIVE_END_TERM  >= rtpTarget.CENSUS_PERIOD
                                ),
                                'Lookup failed'
                           )                    DESCRIPTION
                  INTO  rtpTarget.RESIDENCY_DESC_INOUTSTATE
                  FROM  DUAL;
                If  rtpTarget.RESIDENCY_DESC_TUITION = 'Lookup failed'
                Then
                        intErrorCount   := intErrorCount + 1;
                        bolTransformError
                                        := True;
                        bolError        := True;
                        strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column ' || strAction || ' error',
                                        i_ErrorMessage          => 'Error looking up description for in/out state residency code',
                                        i_ErrorCode             => Null,
                                        i_SourceColumnHeading   => strHeading022,
                                        i_SourceColumnName      => 'COLUMN_022',
                                        i_TargetColumnName      => 'RESIDENCY_DESC_INOUTSTATE',
                                        i_ColumnValue           => recUploadS1.COLUMN_022
                                );
                End If;
        End If;

/*
        strSqlCommand   := '';
this needs more work.  Don't have enough data to meet the requirements
        rtpTarget.COUNTRY_2CHAR_CITIZENSHIP_1   := strCountry2CharUs;
        rtpTarget.COUNTRY_DESCR_CITIZENSHIP_1   := strCountryDescrUs;

        Case
          When  rtpTarget.US_CITIZENSHIP_CODE = 'C'
          Then  rtpTarget.CITIZENSHIP_CODE_1            := strCitizenshipCodeNative;
                rtpTarget.CITIZENSHIP_STATUS_1          := strCitizenshipStatusNative;
          When  rtpTarget.US_CITIZENSHIP_CODE = 'P'
          Then  rtpTarget.CITIZENSHIP_CODE_1            := strCitizenshipCodeNaturalized;
                rtpTarget.CITIZENSHIP_STATUS_1          := strCitizenshipStatusNaturalized;
          When  rtpTarget.US_CITIZENSHIP_CODE = '3'
          Then  rtpTarget.CITIZENSHIP_CODE_1            := strCitizenshipCodePermanent;
                rtpTarget.CITIZENSHIP_STATUS_1          := strCitizenshipStatusPermanent;
          Else  rtpTarget.CITIZENSHIP_CODE_1            := strCitizenshipCodeNon;
                rtpTarget.CITIZENSHIP_STATUS_1          := strCitizenshipStatusNon;
        End;
*/

        If  rtpTarget.EDUCATION_LEVEL_CODE is Null
        Then
                rtpTarget.EDUCATION_LEVEL_DESCR := '';
        Else
                strSqlCommand   := 'Get EDUCATION_LEVEL_DESCR';
                strAction       := 'translation lookup';
                SELECT  NVL(    (
                                SELECT  TRN.DESCRIPTION
                                  FROM  DLSTG_OWNER.TRANSLATION TRN
                                 WHERE  TRN.DATA_NAME            = 'EDUCATION_LEVEL'
                                   AND  TRN.CODE                 = rtpTarget.EDUCATION_LEVEL_CODE
                                   AND  TRN.EFFECTIVE_TERM      <= rtpTarget.CENSUS_PERIOD
                                   AND  TRN.EFFECTIVE_END_TERM  >= rtpTarget.CENSUS_PERIOD
                                ),
                                'Lookup failed'
                           )                    DESCRIPTION
                  INTO  rtpTarget.EDUCATION_LEVEL_DESCR
                  FROM  DUAL;
                If  rtpTarget.EDUCATION_LEVEL_DESCR = 'Lookup failed'
                Then
                        intErrorCount   := intErrorCount + 1;
                        bolTransformError
                                        := True;
                        bolError        := True;
                        strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column ' || strAction || ' error',
                                        i_ErrorMessage          => 'Error looking up description for education level code',
                                        i_ErrorCode             => Null,
                                        i_SourceColumnHeading   => strHeading028,
                                        i_SourceColumnName      => 'COLUMN_028',
                                        i_TargetColumnName      => 'EDUCATION_LEVEL_DESCR',
                                        i_ColumnValue           => recUploadS1.COLUMN_028
                                );
                End If;
        End If;

        strMessage01    := '';

        If  rtpTarget.EDUCATION_LEVEL_CODE = '10'
        Then
                If  rtpTarget.DOCTORATE_TYPE_CODE not in ('P','R')
                Then
                        strMessage01    := 'For Education Level Code 10 Doctorate Type Code must be P or R.';
                End If;
        Else
                If  rtpTarget.DOCTORATE_TYPE_CODE is not null
                Then
                        strMessage01    := 'For Education Level Codes other than 10 Doctorate Type Code must not be specified.';
                End If;
        End If;

        If  strMessage01 is not null
        Then
                intErrorCount   := intErrorCount + 1;
                bolTransformError
                                := True;
                bolError        := True;
                strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                        (
                                i_ErrorSequence         => intErrorCount,
                                i_ErrorDescription      => 'Column validation error',
                                i_ErrorMessage          => strMessage01,
                                i_ErrorCode             => Null,
                                i_SourceColumnHeading   => strHeading029,
                                i_SourceColumnName      => 'COLUMN_029',
                                i_TargetColumnName      => 'DOCTORATE_TYPE_CODE',
                                i_ColumnValue           => 'Education Level Code: '  || rtpTarget.EDUCATION_LEVEL_CODE
                                                        || '  Doctorate Type Code: ' || rtpTarget.DOCTORATE_TYPE_CODE
                        );
        End If;


        If  rtpTarget.ADMIT_TERM_CODE is Null
        Then
                rtpTarget.ADMIT_TERM_DESCR := '';
        Else
                strSqlCommand   := 'Validate ADMIT_TERM_CODE format';
                strYearMin      := '1990';
                strYearMax      := to_char(sysdate,'YYYY');
                Case
                  When  length(rtpTarget.ADMIT_TERM_CODE) <> 5
                  Then  strFailureMessage := 'Admit term is not 5 digits,  Received ' || to_char(length(rtpTarget.ADMIT_TERM_CODE));
                  When  substr(rtpTarget.ADMIT_TERM_CODE,1,4) < strYearMin
                  Then  strFailureMessage := 'Admit term is too early,  Must not be earlier than ' || strYearMin || '.  Received ' || substr(rtpTarget.ADMIT_TERM_CODE,1,4);
                  When  substr(rtpTarget.ADMIT_TERM_CODE,1,4) > strYearMax
                  Then  strFailureMessage := 'Admit term is too late,  Must not be later than ' || strYearMax || '.  Received ' || substr(rtpTarget.ADMIT_TERM_CODE,1,4);
                  When  substr(rtpTarget.ADMIT_TERM_CODE,5,1) not in ('1','2','3','4')
                  Then  strFailureMessage := 'Admit term last digit must be 1, 2, 3 or 4.  Received ' || substr(rtpTarget.ADMIT_TERM_CODE,5,1);
                  Else  strFailureMessage := '';
                End Case;

                If  strFailureMessage <> ''
                Then
                        intErrorCount   := intErrorCount + 1;
                        strAction       := 'format validation';
                        bolTransformError
                                        := True;
                        bolError        := True;
                        strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column ' || strAction || ' error',
                                        i_ErrorMessage          => strFailureMessage,
                                        i_ErrorCode             => Null,
                                        i_SourceColumnHeading   => strHeading033,
                                        i_SourceColumnName      => 'COLUMN_033',
                                        i_TargetColumnName      => 'CENSUS_PERIOD',
                                        i_ColumnValue           => recUploadS1.COLUMN_033
                                );
                End If;

                strSqlCommand   := 'Lookup ADMIT_TERM_DESCR';
                strAction       := 'translation lookup';
/*		Case-45777 July 2020
                SELECT  NVL(    (
                                SELECT  TRN.TERM_DESCRIPTION
                                  FROM  DLMRT_OWNER.CENSUS_PERIOD TRN
                                 WHERE  TRN.CENSUS_PERIOD       = rtpTarget.ADMIT_TERM_CODE
                                ),
                                'Lookup failed'
                           )                    TERM_DESCRIPTION
                  INTO  rtpTarget.ADMIT_TERM_DESCR
                  FROM  DUAL;
*/
				SELECT NVL(  (                                       --Case-45777 July 2020
						SELECT TRN.TERM_DESCRIPTION
						 FROM DLMRT_OWNER.CENSUS_PERIOD TRN
						 WHERE TRN.CENSUS_PERIOD    = (case when rtpTarget.ADMIT_TERM_CODE < '20002'
											 and length(rtpTarget.ADMIT_TERM_CODE) = 5
											then '00000'
											else rtpTarget.ADMIT_TERM_CODE
										  end)
						),
						'Lookup failed'
					  )          TERM_DESCRIPTION
				 INTO rtpTarget.ADMIT_TERM_DESCR
				 FROM DUAL;
        End If;

        If  rtpTarget.PRIMARY_MAJOR_CIP_CODE is Null
        or  intCipYear is Null
        Then
                rtpTarget.STEM_FLAG := 'N';
        Else
                rtpTarget.STEM_FLAG := '';
                strSqlCommand   := 'Get STEM_FLAG';
                strAction       := 'validation';
                SELECT  (
                                SELECT  CIP.UMPO_STEM
                                  FROM  DLMRT_OWNER.CIP CIP
                                 WHERE  CIP.CIP_CODE            = rtpTarget.PRIMARY_MAJOR_CIP_CODE
                                   AND  CIP.CIP_YEAR            = intCipYear
                           )                    STEM_FLAG
                  INTO  rtpTarget.STEM_FLAG
                  FROM  DUAL;

                strSqlCommand   := 'Validate STEM_FLAG';
                If      rtpTarget.STEM_FLAG is null
                Then
                        intErrorCount   := intErrorCount + 1;
                        bolTransformError
                                        := True;
                        bolError        := True;
                        strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column ' || strAction || ' error',
                                        i_ErrorMessage          => 'Primary major CIP code not found in CIP table.  CIP Code: ' || rtpTarget.PRIMARY_MAJOR_CIP_CODE || ', CIP Year: ' || to_char(intCipYear),
                                        i_ErrorCode             => Null,
                                        i_SourceColumnHeading   => strHeading040,
                                        i_SourceColumnName      => 'COLUMN_040',
                                        i_TargetColumnName      => 'STEM_FLAG',
                                        i_ColumnValue           => recUploadS1.COLUMN_040
                                );
                End If;
        End If;


        If  rtpTarget.PRIMARY_MAJOR_SUB_PLAN_CIP_CODE is not Null
        and intCipYear is not Null
        Then
                strSqlCommand   := 'Validate PRIMARY_MAJOR_SUB_PLAN_CIP_CODE';
                strAction       := 'validation';
                SELECT  NVL    (        (
                                        SELECT  'Y'
                                          FROM  DLMRT_OWNER.CIP CIP
                                         WHERE  CIP.CIP_CODE            = rtpTarget.PRIMARY_MAJOR_SUB_PLAN_CIP_CODE
                                           AND  CIP.CIP_YEAR            = intCipYear
                                        ),
                                        'N'
                                )                    CIP_FOUND
                  INTO  strMajorSubPlanCipFoundFlag
                  FROM  DUAL;
                If      strMajorSubPlanCipFoundFlag = 'N'
                Then
                        intErrorCount   := intErrorCount + 1;
                        bolTransformError
                                        := True;
                        bolError        := True;
                        strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column ' || strAction || ' error',
                                        i_ErrorMessage          => 'Primary major sub plan CIP code not found in CIP table.  CIP Code: ' || rtpTarget.PRIMARY_MAJOR_SUB_PLAN_CIP_CODE || ', CIP Year: ' || to_char(intCipYear),
                                        i_ErrorCode             => Null,
                                        i_SourceColumnHeading   => strHeading045,
                                        i_SourceColumnName      => 'COLUMN_045',
                                        i_TargetColumnName      => 'PRIMARY_MAJOR_SUB_PLAN_CIP_CODE',
                                        i_ColumnValue           => recUploadS1.COLUMN_045
                                );
                End If;
        End If;


        If  rtpTarget.ACADEMIC_LEVEL_BOT_CODE is Null
        Then
                rtpTarget.ACADEMIC_LEVEL_BOT_DESCR := '';
        Else
                strSqlCommand   := 'Get ACADEMIC_LEVEL_BOT_DESCR';
                strAction       := 'translation lookup';
                SELECT  NVL(    (
                                SELECT  TRN.DESCRIPTION
                                  FROM  DLSTG_OWNER.TRANSLATION TRN
                                 WHERE  TRN.DATA_NAME            = 'ACADEMIC_LEVEL(BOT)'
                                   AND  TRN.CODE                 = rtpTarget.ACADEMIC_LEVEL_BOT_CODE
                                   AND  TRN.EFFECTIVE_TERM      <= rtpTarget.CENSUS_PERIOD
                                   AND  TRN.EFFECTIVE_END_TERM  >= rtpTarget.CENSUS_PERIOD
                                ),
                                'Lookup failed'
                           )                    DESCRIPTION
                  INTO  rtpTarget.ACADEMIC_LEVEL_BOT_DESCR
                  FROM  DUAL;
                If  rtpTarget.ACADEMIC_LEVEL_BOT_DESCR = 'Lookup failed'
                Then
                        intErrorCount   := intErrorCount + 1;
                        bolTransformError
                                        := True;
                        bolError        := True;
                        strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column ' || strAction || ' error',
                                        i_ErrorMessage          => 'Error looking up description for academic level (BOT) code',
                                        i_ErrorCode             => Null,
                                        i_SourceColumnHeading   => strHeading046,
                                        i_SourceColumnName      => 'COLUMN_046',
                                        i_TargetColumnName      => 'ACADEMIC_LEVEL_BOT_DESCR',
                                        i_ColumnValue           => recUploadS1.COLUMN_046
                                );
                End If;
        End If;

        If  rtpTarget.ACADEMIC_LEVEL_EOT_CODE is Null
        Then
                rtpTarget.ACADEMIC_LEVEL_EOT_DESCR := '';
        Else
                strSqlCommand   := 'Get ACADEMIC_LEVEL_EOT_DESCR';
                strAction       := 'translation lookup';
                SELECT  NVL(    (
                                SELECT  TRN.DESCRIPTION
                                  FROM  DLSTG_OWNER.TRANSLATION TRN
                                 WHERE  TRN.DATA_NAME            = 'ACADEMIC_LEVEL(EOT)'
                                   AND  TRN.CODE                 = rtpTarget.ACADEMIC_LEVEL_EOT_CODE
                                   AND  TRN.EFFECTIVE_TERM      <= rtpTarget.CENSUS_PERIOD
                                   AND  TRN.EFFECTIVE_END_TERM  >= rtpTarget.CENSUS_PERIOD
                                ),
                                'Lookup failed'
                           )                    DESCRIPTION
                  INTO  rtpTarget.ACADEMIC_LEVEL_EOT_DESCR
                  FROM  DUAL;
                If  rtpTarget.ACADEMIC_LEVEL_EOT_DESCR = 'Lookup failed'
                Then
                        intErrorCount   := intErrorCount + 1;
                        bolTransformError
                                        := True;
                        bolError        := True;
                        strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column ' || strAction || ' error',
                                        i_ErrorMessage          => 'Error looking up description for academic level (EOT) code',
                                        i_ErrorCode             => Null,
                                        i_SourceColumnHeading   => strHeading047,
                                        i_SourceColumnName      => 'COLUMN_047',
                                        i_TargetColumnName      => 'ACADEMIC_LEVEL_EOT_DESCR',
                                        i_ColumnValue           => recUploadS1.COLUMN_047
                                );
                End If;
        End If;

        If  rtpTarget.EXPECTED_GRAD_TERM_CODE is Null
        Then
                rtpTarget.EXPECTED_GRAD_TERM_DESCR := '';
        Else
                strSqlCommand   := 'Lookup EXPECTED_GRAD_TERM_DESCR';
                strAction       := 'translation lookup';
/*		Case-45777 July 2020
                SELECT  NVL(    (
                                SELECT  TRN.TERM_DESCRIPTION
                                  FROM  DLMRT_OWNER.CENSUS_PERIOD TRN
                                 WHERE  TRN.CENSUS_PERIOD       = rtpTarget.EXPECTED_GRAD_TERM_CODE
                                ),
                                'Lookup failed'
                           )                    TERM_DESCRIPTION
                  INTO  rtpTarget.EXPECTED_GRAD_TERM_DESCR
                  FROM  DUAL;
*/
				SELECT NVL(  (                                       --Case-45777 July 2020
						SELECT TRN.TERM_DESCRIPTION
						 FROM DLMRT_OWNER.CENSUS_PERIOD TRN
						 WHERE TRN.CENSUS_PERIOD    = (case when rtpTarget.EXPECTED_GRAD_TERM_CODE < '20002'
											 and length(rtpTarget.EXPECTED_GRAD_TERM_CODE) = 5
											then '00000'
											else rtpTarget.EXPECTED_GRAD_TERM_CODE
										  end)
						),
						'Lookup failed'
					  )          TERM_DESCRIPTION
				 INTO rtpTarget.EXPECTED_GRAD_TERM_DESCR
				 FROM DUAL;
                If  rtpTarget.EXPECTED_GRAD_TERM_DESCR = 'Lookup failed'
                Then
                        intErrorCount   := intErrorCount + 1;
                        bolTransformError
                                        := True;
                        bolError        := True;
                        strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column ' || strAction || ' error',
                                        i_ErrorMessage          => 'Error looking up description for expected graduation census period',
                                        i_ErrorCode             => Null,
                                        i_SourceColumnHeading   => strHeading048,
                                        i_SourceColumnName      => 'COLUMN_048',
                                        i_TargetColumnName      => 'EXPECTED_GRAD_TERM_DESCR',
                                        i_ColumnValue           => recUploadS1.COLUMN_048
                                );
                End If;
        End If;

        If  rtpTarget.FT_PT_CODE is Null
        Then
                rtpTarget.FT_PT_DESCR := '';
        Else
                strSqlCommand   := 'Get FT_PT_DESCR 1';
                strAction       := 'translation lookup';
                SELECT  NVL(    (
                                SELECT  TRN.DESCRIPTION
                                  FROM  DLSTG_OWNER.TRANSLATION TRN
                                 WHERE  TRN.DATA_NAME            = 'FT_PT_STATUS'
                                   AND  TRN.CODE                 = rtpTarget.FT_PT_CODE
                                   AND  TRN.EFFECTIVE_TERM      <= rtpTarget.CENSUS_PERIOD
                                   AND  TRN.EFFECTIVE_END_TERM  >= rtpTarget.CENSUS_PERIOD
                                ),
                                'Lookup failed'
                           )                    DESCRIPTION
                  INTO  rtpTarget.FT_PT_DESCR
                  FROM  DUAL;
                strSqlCommand   := 'Get FT_PT_DESCR 2';
                If  rtpTarget.FT_PT_DESCR = 'Lookup failed'
                Then
                        intErrorCount   := intErrorCount + 1;
                        bolTransformError
                                        := True;
                        bolError        := True;
                        strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column ' || strAction || ' error',
                                        i_ErrorMessage          => 'Error looking up description for academic load code',
                                        i_ErrorCode             => Null,
                                        i_SourceColumnHeading   => strHeading051,
                                        i_SourceColumnName      => 'COLUMN_051',
                                        i_TargetColumnName      => 'FT_PT_DESCR',
                                        i_ColumnValue           => recUploadS1.COLUMN_051
                                );
                End If;
        End If;

        If  i_Institution not in ('UMBOS', 'UMDAR', 'UMLOW')
        and rtpTarget.STATE_PERM_ADDRESS = 'MA'
        Then
                Begin
                        strZipCode      := substr(rtpTarget.POSTAL_CODE_PERM_ADDRESS,1,5);
                        strSqlCommand   := 'Lookup ZIP_CODE_MA';
                        strAction       := 'county lookup';
                        SELECT  NVL(    (
                                        SELECT  ZIP.COUNTY
                                          FROM  DLSTG_OWNER.ZIP_CODE_MA ZIP
                                         WHERE  ZIP.ZIP_CODE_5DIGIT     = strZipCode
                                        ),
                                        'Not found - MA county for ZIP ' || strZipCode
                                   )                    DESCRIPTION
                          INTO  rtpTarget.COUNTY_PERM_ADDRESS
                          FROM  DUAL;
                EXCEPTION WHEN OTHERS THEN
                        rtpTarget.COUNTY_PERM_ADDRESS   := 'No MA county for ZIP ' || rtpTarget.POSTAL_CODE_PERM_ADDRESS;
                End;
        End If;

        strSqlCommand   := 'End of transformations and validations';

--      If there were no column transformation errors then insert the target row.
        If  not bolError
        Then
                Begin
                        INSERT  /*+APPEND*/
                          INTO  DLSTG_OWNER.ENROLLMENT_S2
                                (
                                PROCESS_SID,
                                PROCESS_START_DATE,
                                RECORD_NUMBER,
                                INSTITUTION,
                                CAMPUS,
                                CENSUS_PERIOD,
                                CENSUS_PERIOD_DESCR,
                                CENSUS_SEQ,
                                PERSON_ID,
                                UMASS_GUID,
                                BIRTH_DATE,
                                CAMPUS_CENSUS_DATE,
                                IPEDS_AGE_RANGE_CODE,
                                IPEDS_AGE_RANGE_DESCR,
                                GENDER_CODE,
                                GENDER_DESCR,
                                US_CITIZENSHIP_CODE,
                                US_CITIZENSHIP_STATUS,
                                COUNTRY_2CHAR_NON_US,
                                COUNTRY_DESCR_NON_US,
                                FED_ETHNICITY_CODE,
                                FED_ETHNICITY_DESCR,
                                STATE_ETHNICITY_CODE,
                                STATE_ETHNICITY_DESCR,
                                UNDER_REPRESENTED_MINORITY,
                                MILITARY_STATUS,
                                COUNTRY_2CHAR_PERM_ADDRESS,
                                COUNTRY_DESCR_PERM_ADDRESS,
                                STATE_PERM_ADDRESS,
                                POSTAL_CODE_PERM_ADDRESS,
                                COUNTY_PERM_ADDRESS,
                                CAMPUS_RESIDENT,
                                RESIDENCY_CODE_SA_TUITION,
                                RESIDENCY_DESC_SA_TUITION,
                                RESIDENCY_CODE_TUITION,
                                RESIDENCY_DESC_TUITION,
				RESIDENCY_CODE_INOUTSTATE,
				RESIDENCY_DESC_INOUTSTATE,
				COUNTRY_2CHAR_NON_US_1,
                                COUNTRY_DESCR_NON_US_1,
                                CITIZENSHIP_CODE_NON_US_1,
                                CITIZENSHIP_STATUS_NON_US_1,
                                COUNTRY_2CHAR_NON_US_2,
                                COUNTRY_DESCR_NON_US_2,
                                CITIZENSHIP_CODE_NON_US_2,
                                CITIZENSHIP_STATUS_NON_US_2,
                                COUNTRY_2CHAR_NON_US_3,
                                COUNTRY_DESCR_NON_US_3,
                                CITIZENSHIP_CODE_NON_US_3,
                                CITIZENSHIP_STATUS_NON_US_3,
                                ACADEMIC_CAREER_PS,
                                STUDENT_CAREER_NUMBER_PS,
                                TERM_CODE_PS,
                                HEADCOUNT,
                                CAREER_LEVEL_CODE,
                                PROGRAM_TYPE,
                                EDUCATION_LEVEL_CODE,
                                EDUCATION_LEVEL_DESCR,
                                DOCTORATE_TYPE_CODE,
                                CES_STUDENT_FLAG,
                                REPORTED_TO_IPEDS_FLAG,
                                ADMIT_TYPE,
                                ADMIT_TERM_CODE,
                                ADMIT_TERM_DESCR,
                                FIRST_GENERATION_FLAG,
                                NEW_OR_CONTINUING,
                                PRIMARY_COLLEGE_CODE,
                                PRIMARY_COLLEGE_DESCR,
                                PRIMARY_MAJOR_CODE,
                                PRIMARY_MAJOR_DESCR,
                                PRIMARY_MAJOR_CIP_CODE,
                                PRIMARY_MAJOR_PLAN_TYPE,
                                STEM_FLAG,
                                PRIMARY_MAJOR_SUB_PLAN_TYPE,
                                PRIMARY_MAJOR_SUB_PLAN_CODE,
                                PRIMARY_MAJOR_SUB_PLAN_DESCR,
                                PRIMARY_MAJOR_SUB_PLAN_CIP_CODE,
                                ACADEMIC_LEVEL_BOT_CODE,
                                ACADEMIC_LEVEL_BOT_DESCR,
                                ACADEMIC_LEVEL_EOT_CODE,
                                ACADEMIC_LEVEL_EOT_DESCR,
                                EXPECTED_GRAD_TERM_CODE,
                                EXPECTED_GRAD_TERM_DESCR,
                                CUMULATIVE_CREDITS,
                                CUMULATIVE_GPA,
                                FT_PT_CODE,
                                FT_PT_DESCR,
                                TOTAL_CREDITS,
                                ONLINE_CREDITS,
                                NON_ONLINE_CREDITS,
                                ONLINE_ONLY_STUDENT_FLAG,
                                MIXED_MODE_INSTRUCTION_FLAG,
                                CE_CREDITS,
                                NON_CE_CREDITS,
                                TOTAL_FTE,
                                ONLINE_FTE,
                                CE_FTE,
                                CLASS_COUNT,
                                ONLINE_CLASS_COUNT,
                                CE_CLASS_COUNT,
                                ALL_REGISTRATION_COUNT,
                                ONLINE_REGISTRATION_COUNT,
                                CE_REGISTRATION_COUNT,
                                PELL_RECIPIENT_FLAG,
                                INSERT_TIME
                                )
                        VALUES
                                (
                                intProcessSid,
                                dtProcessStart,
                                recUploadS1.RECORD_NUMBER,
                                rtpTarget.INSTITUTION,
                                rtpTarget.CAMPUS,
                                rtpTarget.CENSUS_PERIOD,
                                rtpTarget.CENSUS_PERIOD_DESCR,
                                rtpTarget.CENSUS_SEQ,
                                rtpTarget.PERSON_ID,
                                rtpTarget.UMASS_GUID,
                                rtpTarget.BIRTH_DATE,
                                rtpTarget.CAMPUS_CENSUS_DATE,
                                rtpTarget.IPEDS_AGE_RANGE_CODE,
                                rtpTarget.IPEDS_AGE_RANGE_DESCR,
                                rtpTarget.GENDER_CODE,
                                rtpTarget.GENDER_DESCR,
                                rtpTarget.US_CITIZENSHIP_CODE,
                                rtpTarget.US_CITIZENSHIP_STATUS,
                                rtpTarget.COUNTRY_2CHAR_NON_US,
                                rtpTarget.COUNTRY_DESCR_NON_US,
                                rtpTarget.FED_ETHNICITY_CODE,
                                rtpTarget.FED_ETHNICITY_DESCR,
                                rtpTarget.STATE_ETHNICITY_CODE,
                                rtpTarget.STATE_ETHNICITY_DESCR,
                                rtpTarget.UNDER_REPRESENTED_MINORITY,
                                rtpTarget.MILITARY_STATUS,
                                rtpTarget.COUNTRY_2CHAR_PERM_ADDRESS,
                                rtpTarget.COUNTRY_DESCR_PERM_ADDRESS,
                                rtpTarget.STATE_PERM_ADDRESS,
                                rtpTarget.POSTAL_CODE_PERM_ADDRESS,
                                rtpTarget.COUNTY_PERM_ADDRESS,
                                rtpTarget.CAMPUS_RESIDENT,
                                rtpTarget.RESIDENCY_CODE_SA_TUITION,
                                rtpTarget.RESIDENCY_DESC_SA_TUITION,
                                rtpTarget.RESIDENCY_CODE_TUITION,
                                rtpTarget.RESIDENCY_DESC_TUITION,
				rtpTarget.RESIDENCY_CODE_INOUTSTATE,
				rtpTarget.RESIDENCY_DESC_INOUTSTATE,
                                rtpTarget.COUNTRY_2CHAR_NON_US_1,
                                rtpTarget.COUNTRY_DESCR_NON_US_1,
                                rtpTarget.CITIZENSHIP_CODE_NON_US_1,
                                rtpTarget.CITIZENSHIP_STATUS_NON_US_1,
                                rtpTarget.COUNTRY_2CHAR_NON_US_2,
                                rtpTarget.COUNTRY_DESCR_NON_US_2,
                                rtpTarget.CITIZENSHIP_CODE_NON_US_2,
                                rtpTarget.CITIZENSHIP_STATUS_NON_US_2,
                                rtpTarget.COUNTRY_2CHAR_NON_US_3,
                                rtpTarget.COUNTRY_DESCR_NON_US_3,
                                rtpTarget.CITIZENSHIP_CODE_NON_US_3,
                                rtpTarget.CITIZENSHIP_STATUS_NON_US_3,
                                rtpTarget.ACADEMIC_CAREER_PS,
                                rtpTarget.STUDENT_CAREER_NUMBER_PS,
                                rtpTarget.TERM_CODE_PS,
                                rtpTarget.HEADCOUNT,
                                rtpTarget.CAREER_LEVEL_CODE,
                                rtpTarget.PROGRAM_TYPE,
                                rtpTarget.EDUCATION_LEVEL_CODE,
                                rtpTarget.EDUCATION_LEVEL_DESCR,
                                rtpTarget.DOCTORATE_TYPE_CODE,
                                rtpTarget.CES_STUDENT_FLAG,
                                rtpTarget.REPORTED_TO_IPEDS_FLAG,
                                rtpTarget.ADMIT_TYPE,
                                rtpTarget.ADMIT_TERM_CODE,
                                rtpTarget.ADMIT_TERM_DESCR,
                                rtpTarget.FIRST_GENERATION_FLAG,
                                rtpTarget.NEW_OR_CONTINUING,
                                rtpTarget.PRIMARY_COLLEGE_CODE,
                                rtpTarget.PRIMARY_COLLEGE_DESCR,
                                rtpTarget.PRIMARY_MAJOR_CODE,
                                rtpTarget.PRIMARY_MAJOR_DESCR,
                                rtpTarget.PRIMARY_MAJOR_CIP_CODE,
                                rtpTarget.PRIMARY_MAJOR_PLAN_TYPE,
                                rtpTarget.STEM_FLAG,
                                rtpTarget.PRIMARY_MAJOR_SUB_PLAN_TYPE,
                                rtpTarget.PRIMARY_MAJOR_SUB_PLAN_CODE,
                                rtpTarget.PRIMARY_MAJOR_SUB_PLAN_DESCR,
                                rtpTarget.PRIMARY_MAJOR_SUB_PLAN_CIP_CODE,
                                rtpTarget.ACADEMIC_LEVEL_BOT_CODE,
                                rtpTarget.ACADEMIC_LEVEL_BOT_DESCR,
                                rtpTarget.ACADEMIC_LEVEL_EOT_CODE,
                                rtpTarget.ACADEMIC_LEVEL_EOT_DESCR,
                                rtpTarget.EXPECTED_GRAD_TERM_CODE,
                                rtpTarget.EXPECTED_GRAD_TERM_DESCR,
                                rtpTarget.CUMULATIVE_CREDITS,
                                rtpTarget.CUMULATIVE_GPA,
                                rtpTarget.FT_PT_CODE,
                                rtpTarget.FT_PT_DESCR,
                                rtpTarget.TOTAL_CREDITS,
                                rtpTarget.ONLINE_CREDITS,
                                rtpTarget.NON_ONLINE_CREDITS,
                                rtpTarget.ONLINE_ONLY_STUDENT_FLAG,
                                rtpTarget.MIXED_MODE_INSTRUCTION_FLAG,
                                rtpTarget.CE_CREDITS,
                                rtpTarget.NON_CE_CREDITS,
                                rtpTarget.TOTAL_FTE,
                                rtpTarget.ONLINE_FTE,
                                rtpTarget.CE_FTE,
                                rtpTarget.CLASS_COUNT,
                                rtpTarget.ONLINE_CLASS_COUNT,
                                rtpTarget.CE_CLASS_COUNT,
                                rtpTarget.ALL_REGISTRATION_COUNT,
                                rtpTarget.ONLINE_REGISTRATION_COUNT,
                                rtpTarget.CE_REGISTRATION_COUNT,
                                rtpTarget.PELL_RECIPIENT_FLAG,
                                SYSDATE
                                )
                        ;

                        EXCEPTION
                          WHEN OTHERS THEN
                                -- If we get here then the error should be something other than
                                -- a bad column value.  Unique constraint violation is one possibility.
                                intErrorCount   := intErrorCount + 1;
                                bolError        := True;
                                strSqlCommand   := 'SMT_INTERFACE.LOG_ERROR';
                                COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                        (
                                                i_ErrorSequence         => intErrorCount,
                                                i_ErrorDescription      => 'Row insert error',
                                                i_ErrorMessage          => COMMON_OWNER.SMT_INTERFACE.MESSAGE_TRANSLATE(i_SqlCode=>SQLCODE, i_ErrorMessage=>SQLERRM),
                                                i_ErrorCode             => SQLCODE,
                                                i_SourceColumnHeading   => 'Stage row insert error',
                                                i_ColumnValue           =>         'Institituon: '       || rtpTarget.INSTITUTION
                                                                                || ', Census Period: '   || rtpTarget.CENSUS_PERIOD
                                                                                || ', Census Sequence: ' || to_char(rtpTarget.CENSUS_SEQ)
                                                                                || ', Person ID: '       || rtpTarget.PERSON_ID
                                        );
                END;
        End If;
        If  bolError
        Then
                strSqlCommand   := 'INSERT INTO DLSTG_OWNER.REJECT_LOG';
                INSERT
                  INTO  DLSTG_OWNER.REJECT_LOG
                        (
                        UPLOAD_ID,
                        ROW_NUMBER,
                        INSTITUTION,
                        CENSUS_PERIOD,
                        CENSUS_SEQ,
                        INSERT_TIME
                        )
                SELECT
                        strUploadId             UPLOAD_ID,
                        intRowNum               ROW_NUMBER,
                        rtpTarget.INSTITUTION   INSTITUTION,
                        rtpTarget.CENSUS_PERIOD CENSUS_PERIOD,
                        rtpTarget.CENSUS_SEQ    CENSUS_SEQ,
                        SYSDATE                 INSERT_TIME
                  FROM  DUAL
                ;
                intFailedRowCount := intFailedRowCount + 1;
        Else
                intInsertCount    := intInsertCount + 1;
        End If;

        -- If the maximum number of failed rows is exceeded then stop getting source rows.
        If  intFailedRowCount > intFailedRowMax
        Then
                COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount + 1,
                                        i_ErrorDescription      => 'Termination before end of data',
                                        i_ErrorMessage          => 'Maximum number of failed rows exceeded',
                                        i_ErrorCode             => Null
                                );
                bolPrematureExit := True;
                strPrematureExit := 'TRUE';
                Exit;
        End If;
    End If;
End Loop; --recUploadS1

strMessage01    := '# rows inserted: ' || TO_CHAR(intInsertCount);
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'COMMIT';
COMMIT;

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName       => 'ENROLLMENT_S2',
                i_Action                => 'INSERT',
                i_RowCount              => intInsertCount,
                i_Comments              => 'Partition: ' || strPartitionName
        );

strSqlCommand   := 'SMT_INDEX.ALL_REBUILD';
COMMON_OWNER.SMT_INDEX.ALL_REBUILD
        (
                i_TableOwner            => 'DLSTG_OWNER',
                i_TableName             => 'ENROLLMENT_S2',
                i_UnusableOnly          => True,
                i_AllJoinedTables       => False,
                i_ParallelDegree        => 4
        );

intS2InsertCount        := intInsertCount;

If  bolPrematureExit  -- Insert a reject log row for each distinct census in the upload.
Then
        strMessage01    := 'Premature exit.  Inserting REJECT_LOG row for each census...';
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
        INSERT
          INTO  DLSTG_OWNER.REJECT_LOG
                (
                UPLOAD_ID,
                ROW_NUMBER,
                INSTITUTION,
                CENSUS_PERIOD,
                CENSUS_SEQ,
                INSERT_TIME
                )
        SELECT  DISTINCT
                CST.UPLOAD_ID,
                Null                    ROW_NUMBER,
                CST.INSTITUTION,
                CST.CENSUS_PERIOD,
                CST.CENSUS_SEQ,
                SYSDATE                 INSERT_TIME
          FROM  DLMRT_OWNER.CENSUS_STATUS       CST
         WHERE  CST.UPLOAD_ID                   = strUploadId
           AND  CST.UPLOAD_TYPE                 = strUploadType
           AND  CST.READY_FOR_STAGE             = 'Y'
        ;

        intRowCount     := SQL%ROWCOUNT;

        strSqlCommand   := 'COMMIT';
        COMMIT;

        strMessage01    := '# rows inserted: ' || TO_CHAR(intRowCount);
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

        strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
                (
                        i_TargetTableName       => 'REJECT_LOG',
                        i_Action                => 'INSERT',
                        i_RowCount              => intRowCount,
                        i_Comments              => '1 row for all periods'
                );
End If;

intInsertCount  := 0;
intUpdateCount  := 0;

strSqlCommand   := 'For recCensusPeriod';
For recCensusPeriod in
        (
        SELECT  INL.INSTITUTION,
                INL.CENSUS_PERIOD,
                INL.CENSUS_SEQ,
                INL.STAGE_STATUS,
                INL.STAGE_ROW_COUNT
          FROM
                (
                SELECT
                        ES2.INSTITUTION,
                        ES2.CENSUS_PERIOD,
                        ES2.CENSUS_SEQ,
                        CASE
--                          WHEN  strPrematureExit = 'TRUE'
--                          THEN  'REJECTED'
                          WHEN  RLG.INSTITUTION IS NULL
                          THEN  'LOADED'
                          ELSE  'REJECTED'
                        END                     STAGE_STATUS,
                        ES2.STAGE_ROW_COUNT
                  FROM  (
                        SELECT  INSTITUTION,
                                CENSUS_PERIOD,
                                CENSUS_SEQ,
                                SUM(STAGE_ROW_COUNT)    STAGE_ROW_COUNT
                          FROM  (
                                SELECT  INSTITUTION,
                                        CENSUS_PERIOD,
                                        CENSUS_SEQ,
                                        COUNT(*)        STAGE_ROW_COUNT
                                  FROM  DLSTG_OWNER.ENROLLMENT_S2
                                 WHERE  INSTITUTION     = i_Institution
                                   AND  PROCESS_SID     = intProcessSid
                                GROUP BY
                                        INSTITUTION,
                                        CENSUS_PERIOD,
                                        CENSUS_SEQ
                                UNION ALL
                                SELECT  DISTINCT
                                        INSTITUTION,
                                        CENSUS_PERIOD,
                                        CENSUS_SEQ,
                                        0               STAGE_ROW_COUNT
                                  FROM  DLSTG_OWNER.REJECT_LOG
                                 WHERE  UPLOAD_ID       = strUploadId
                                )
                                GROUP BY
                                        INSTITUTION,
                                        CENSUS_PERIOD,
                                        CENSUS_SEQ
                        ) ES2,
                        (
                        SELECT  DISTINCT
                                INSTITUTION,
                                CENSUS_PERIOD,
                                CENSUS_SEQ
                          FROM  DLSTG_OWNER.REJECT_LOG
                         WHERE  UPLOAD_ID       = strUploadId
                        ) RLG
                 WHERE  RLG.INSTITUTION(+)      = ES2.INSTITUTION
                   AND  RLG.CENSUS_PERIOD(+)    = ES2.CENSUS_PERIOD
                   AND  RLG.CENSUS_SEQ(+)       = ES2.CENSUS_SEQ
                ) INL
         WHERE  INL.INSTITUTION IS NOT NULL
        ORDER BY
                INL.INSTITUTION,
                INL.CENSUS_PERIOD,
                INL.CENSUS_SEQ
        )
Loop
        strSqlCommand   := 'SELECT strStatusRowExists';
        SELECT  (
                NVL     (       (
                                SELECT  MAX('Y')
                                  FROM  DLMRT_OWNER.CENSUS_STATUS CST
                                 WHERE  CST.UPLOAD_TYPE         = strUploadType
                                   AND  CST.INSTITUTION         = recCensusPeriod.INSTITUTION
                                   AND  CST.CENSUS_PERIOD       = recCensusPeriod.CENSUS_PERIOD
                                   AND  CST.CENSUS_SEQ          = recCensusPeriod.CENSUS_SEQ
                                ),
                                'N'
                        )
                )       ROW_EXISTS
          INTO  strStatusRowExists
          FROM  DUAL;
        If  strStatusRowExists = 'Y'
        Then
                strMessage01    := 'Updating DLMRT_OWNER.CENSUS_STATUS (INSTITUTION:' || recCensusPeriod.INSTITUTION || ' CENSUS_PERIOD:' || recCensusPeriod.CENSUS_PERIOD || ' CENSUS_SEQ: ' || to_char(recCensusPeriod.CENSUS_SEQ) || ' STAGE_STATUS: ' || recCensusPeriod.STAGE_STATUS;
                COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

                strSqlCommand   := 'UPDATE DLMRT_OWNER.CENSUS_STATUS';
                UPDATE  DLMRT_OWNER.CENSUS_STATUS
                   SET  FILE_NAME               = strFileName,
                        STAGE_STATUS            = recCensusPeriod.STAGE_STATUS,
                        APPROVED_FOR_CONFIRM    = 'N',
                        STAGE_LOAD_TIME         = dtProcessStart,
                        STAGE_ROW_COUNT         = recCensusPeriod.STAGE_ROW_COUNT,
                        LAST_UPDATE_TIME        = SYSDATE,
                        LAST_UPDATE_BY          = i_ProcessName
                 WHERE  UPLOAD_TYPE     = strUploadType
                   AND  INSTITUTION     = recCensusPeriod.INSTITUTION
                   AND  CENSUS_PERIOD   = recCensusPeriod.CENSUS_PERIOD
                   AND  CENSUS_SEQ      = recCensusPeriod.CENSUS_SEQ
                ;
                intUpdateCount  := intUpdateCount + SQL%ROWCOUNT;
        Else
                strMessage01    := 'Inserting DLMRT_OWNER.CENSUS_STATUS row (INSTITUTION:' || recCensusPeriod.INSTITUTION || ' CENSUS_PERIOD:' || recCensusPeriod.CENSUS_PERIOD || ' CENSUS_SEQ: ' || to_char(recCensusPeriod.CENSUS_SEQ);
                COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

                strSqlCommand   := 'INSERT DLMRT_OWNER.CENSUS_STATUS';
                INSERT
                  INTO  DLMRT_OWNER.CENSUS_STATUS
                        (
                        UPLOAD_TYPE,
                        INSTITUTION,
                        CENSUS_PERIOD,
                        CENSUS_SEQ,
                        FILE_NAME,
                        UPLOAD_ID,
                        UPLOAD_STATUS,
                        STAGE_STATUS,
                        PRELIM_STATUS,
                        CONFIRMED_STATUS,
                        APPROVED_FOR_CONFIRM,
                        UPLOAD_TIME,
                        STAGE_LOAD_TIME,
                        PRELIM_LOAD_TIME,
                        CONFIRMED_LOAD_TIME,
                        UPLOAD_ROW_COUNT,
                        STAGE_ROW_COUNT,
                        PRELIM_ROW_COUNT,
                        CONFIRMED_ROW_COUNT,
                        LAST_UPDATE_BY,
                        LAST_UPDATE_TIME
                        )
                SELECT
                        strUploadType                           UPLOAD_TYPE,
                        recCensusPeriod.INSTITUTION             INSTITUTION,
                        recCensusPeriod.CENSUS_PERIOD           CENSUS_PERIOD,
                        recCensusPeriod.CENSUS_SEQ              CENSUS_SEQ,
                        strFileName                             FILE_NAME,
                        strUploadId                             UPLOAD_ID,
                        Null                                    UPLOAD_STATUS,
                        recCensusPeriod.STAGE_STATUS            STAGE_STATUS,
                        Null                                    PRELIMINARY_STATUS,
                        Null                                    CONFIRMED_STATUS,
                        'N'                                     APPROVED_FOR_CONFIRM,
                        Null                                    UPLOAD_TIME,
                        dtProcessStart                          STAGE_LOAD_TIME,
                        Null                                    PRELIMINARY_LOAD_TIME,
                        Null                                    CONFIRMED_LOAD_TIME,
                        Null                                    UPLOAD_ROW_COUNT,
                        recCensusPeriod.STAGE_ROW_COUNT         STAGE_ROW_COUNT,
                        Null                                    PRELIM_ROW_COUNT,
                        Null                                    CONFIRMED_ROW_COUNT,
                        i_ProcessName                           LAST_UPDATE_BY,
                        SYSDATE                                 LAST_UPDATE_TIME
                  FROM  DUAL
                ;
                intInsertCount  := intInsertCount + SQL%ROWCOUNT;
        End If;
End Loop; -- recCensusPeriod

strSqlCommand   := 'COMMIT';
COMMIT;

strMessage01    := '# of rows updated: ' || TO_CHAR(intUpdateCount);
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strMessage01    := '# of rows inserted: ' || TO_CHAR(intInsertCount);
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'CENSUS_STATUS',
                i_Action            => 'UPDATE',
                i_RowCount          => intUpdateCount
        );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'CENSUS_STATUS',
                i_Action            => 'INSERT',
                i_RowCount          => intInsertCount
        );

strMessage01    := 'Gathering statistics for DLSTG_OWNER.ENROLLMENT_S2...';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'DBMS_STATS.GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS
        (
                OWNNAME                 => 'DLSTG_OWNER',
                TABNAME                 => 'ENROLLMENT_S2',
                DEGREE                  => 8,
                ESTIMATE_PERCENT        => DBMS_STATS.AUTO_SAMPLE_SIZE,
                METHOD_OPT              => 'FOR ALL COLUMNS SIZE AUTO',
                GRANULARITY             => 'AUTO',
                FORCE                   => True,
                NO_INVALIDATE           => False
        );

strMessage01    := 'Statistics gathered.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

If  intErrorCount = 0
Then
        strSqlCommand   := 'SMT_INTERFACE.INTERFACE_SUCCESS';
        COMMON_OWNER.SMT_INTERFACE.INTERFACE_SUCCESS
                (       i_SuccessRowCount       => intS2InsertCount,
                        i_FailedRowCount        => intFailedRowCount
                );

        strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

        strMessage01    := i_ProcessName || ' is complete.';
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

Else
--      There were one or more errors. Mark the interface as failed.
        strSqlCommand   := 'SMT_INTERFACE.INTERFACE_FAILURE';
        COMMON_OWNER.SMT_INTERFACE.INTERFACE_FAILURE
                (       i_SuccessRowCount       => intS2InsertCount,
                        i_FailedRowCount        => intFailedRowCount,
                        i_EofProcessed          => Case When bolPrematureExit Then 'N' Else 'Y' End
                );
End If;


EXCEPTION
    WHEN OTHERS THEN
        numSqlCode := SQLCODE;
        strSqlErrm := SQLERRM;

        intErrorCount   := intErrorCount + 1;
        bolError        := True;
        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                (       i_ErrorSequence         => intErrorCount,
                        i_ErrorDescription      => 'General error',
                        i_ErrorMessage          => strSqlErrm,
                        i_ErrorCode             => numSqlCode
                );

        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => numSqlCode,
                        i_SqlErrm      => strSqlErrm
                );


END "EnrollmentS2";
/

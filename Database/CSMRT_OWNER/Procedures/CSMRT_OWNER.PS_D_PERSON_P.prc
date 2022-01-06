CREATE OR REPLACE PROCEDURE             "PS_D_PERSON_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads target table PS_D_PERSON
-- PS_D_PERSON -- Replacement for PS_D_PERSON, UM_D_PERSON, PS_D_PERSON_ATTR
-- V01  SMT-xxxx 3/28/2018,    srikanth ,pabbu converted to proc from sql
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_PERSON';
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

strMessage01    := 'Merging data into CSSTG_OWNER.PS_D_PERSON';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_D_PERSON';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_PERSON T
using (
with X as (
select /*+ inline parallel(8) */
       FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID,
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN,
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       Q1 as (
select /*+ inline parallel(8) */
       P.EMPLID PERSON_ID, P.SRC_SYS_ID,
       case when P.BIRTHDATE is NULL or P.BIRTHDATE <= '01-JAN-1901' then NULL else P.BIRTHDATE end BIRTH_DT,
       P.BIRTHPLACE BIRTH_PLACE,
       P.BIRTHSTATE BIRTH_STATE, nvl(trim(S.DESCR),'-') BIRTH_STATE_LD,
       P.BIRTHCOUNTRY BIRTH_COUNTRY,
       nvl(trim(C.DESCRSHORT),'-') BIRTH_COUNTRY_SD,
       nvl(trim(C.DESCR),'-') BIRTH_COUNTRY_LD,
       nvl(trim(C.COUNTRY_2CHAR),'-') BIRTH_COUNTRY_2CHAR,
       nvl(trim(C.EU_MEMBER_STATE),'-') BIRTH_COUNTRY_EU_MEMBER,
       case when P.DT_OF_DEATH <= '01-JAN-1901' then NULL else P.DT_OF_DEATH end DEATH_DT,
       case when P.DT_OF_DEATH is NULL then 'N'
            when P.DT_OF_DEATH > SYSDATE then 'N'
            when P.DT_OF_DEATH <= '01-JAN-1901' then 'N'
            else 'Y'
        end DEATH_FLG,
       P.DATA_ORIGIN
  from CSSTG_OWNER.PS_PERSON P
  left outer join CSSTG_OWNER.PS_COUNTRY_TBL C
    on P.BIRTHCOUNTRY = C.COUNTRY
   and P.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join CSSTG_OWNER.PS_STATE_TBL S
    on P.BIRTHCOUNTRY = S.COUNTRY
   and P.BIRTHSTATE = S.STATE
   and P.SRC_SYS_ID = S.SRC_SYS_ID
   and S.DATA_ORIGIN <> 'D'
 where trim(P.EMPLID) between '00000000' and '99999999'
   and length(trim(P.EMPLID)) = 8),
       Q2 as (
select /*+ inline parallel(8) */
       EMPLID, ADDRESS_TYPE, EFFDT, SRC_SYS_ID,
       COUNTRY, ADDRESS1, ADDRESS2, ADDRESS3, ADDRESS4,
       CITY, STATE, POSTAL,
       row_number() over (partition by EMPLID, ADDRESS_TYPE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_ADDRESSES
 where DATA_ORIGIN <> 'D'),
Q3 as (
select /*+ inline parallel(8) */
       E.EMPLID PERSON_ID,
       replace(trim(E.CONTACT_NAME),'  ',' ') EMERG_CNTCT_NM,
       E.SRC_SYS_ID,
       E.RELATIONSHIP EMERG_RELATIONSHIP, nvl(X1.XLATSHORTNAME,'-') EMERG_RELATIONSHIP_SD, nvl(X1.XLATLONGNAME,'-') EMERG_RELATIONSHIP_LD,
       case when E.SAME_ADDRESS_EMPL = 'Y' and E.ADDRESS1 = '-' then coalesce(ADDR.ADDRESS1, E.ADDRESS1) else E.ADDRESS1 end EMERG_ADDR1_LD,
       case when E.SAME_ADDRESS_EMPL = 'Y' and E.ADDRESS1 = '-' then coalesce(ADDR.ADDRESS2, E.ADDRESS2) else E.ADDRESS2 end EMERG_ADDR2_LD,
       case when E.SAME_ADDRESS_EMPL = 'Y' and E.ADDRESS1 = '-' then coalesce(ADDR.ADDRESS3, E.ADDRESS3) else E.ADDRESS3 end EMERG_ADDR3_LD,
       case when E.SAME_ADDRESS_EMPL = 'Y' and E.ADDRESS1 = '-' then coalesce(ADDR.ADDRESS4, E.ADDRESS4) else E.ADDRESS4 end EMERG_ADDR4_LD,
       case when E.SAME_ADDRESS_EMPL = 'Y' and E.ADDRESS1 = '-' then coalesce(ADDR.CITY, E.CITY) else E.CITY end EMERG_CITY_NM,
       case when E.SAME_ADDRESS_EMPL = 'Y' and E.ADDRESS1 = '-' then coalesce(ADDR.STATE, E.STATE) else E.STATE end EMERG_STATE_CD,
       case when E.SAME_ADDRESS_EMPL = 'Y' and E.ADDRESS1 = '-' then coalesce(ADDR.POSTAL, E.POSTAL) else E.POSTAL end EMERG_POSTAL_CD,
       case when E.SAME_ADDRESS_EMPL = 'Y' and E.ADDRESS1 = '-' then coalesce(ADDR.COUNTRY, E.COUNTRY) else E.COUNTRY end EMERG_CNTRY_CD,
       case when E.SAME_PHONE_EMPL = 'Y' and E.PHONE = '-' then coalesce(P.COUNTRY_CODE, E.COUNTRY_CODE) else E.COUNTRY_CODE end EMERG_COUNTRY_CODE,
       case when E.SAME_PHONE_EMPL = 'Y' and E.PHONE = '-' then coalesce(P.PHONE, E.PHONE) else E.PHONE end EMERG_PHONE_NUM,
       row_number() over (partition by E.EMPLID, E.SRC_SYS_ID
                              order by E.PRIMARY_CONTACT desc, E.CONTACT_NAME) Q_ORDER
  from CSSTG_OWNER.PS_EMERGENCY_CNTCT E
  left outer join CSSTG_OWNER.PS_PERSONAL_PHONE P
    on E.EMPLID = P.EMPLID
   and E.PHONE_TYPE = P.PHONE_TYPE
   and E.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
  left outer join Q2 ADDR
    on E.EMPLID = ADDR.EMPLID
   and E.ADDRESS_TYPE = ADDR.ADDRESS_TYPE
   and E.SRC_SYS_ID = ADDR.SRC_SYS_ID
   and ADDR.Q_ORDER = 1
  left outer join X X1
    on E.RELATIONSHIP = X1.FIELDVALUE
   and E.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'RELATIONSHIP'
   and X1.X_ORDER = 1
 where E.DATA_ORIGIN <> 'D'),
       Q4 as (
select /*+ inline parallel(8) */
       EMPLID PERSON_ID, SRC_SYS_ID,
       VA_BENEFIT, FERPA FERPA_FLG, PLACE_OF_DEATH DEATH_PLACE
  from CSSTG_OWNER.PS_PERSON_SA
 where DATA_ORIGIN <> 'D'),
       Q5 as (
select /*+ inline parallel(8) */
       EMPLID PERSON_ID, SRC_SYS_ID,
       MAR_STATUS MAR_STAT_CD, MAR_STATUS_DT MAR_STAT_DT, SEX GENDER_CD,
       HIGHEST_EDUC_LVL HI_EDU_LVL_CD,
       row_number() over (partition by EMPLID, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_PERS_DATA_EFFDT
 where DATA_ORIGIN <> 'D'),
       Q6 as (
select /*+ inline parallel(8) */
       EMPLID PERSON_ID, SRC_SYS_ID,
       US_WORK_ELIGIBILTY US_WORK_ELIG_IND, MILITARY_STATUS MIL_STAT_CD,
       row_number() over (partition by EMPLID, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_PERS_DATA_USA
 where DATA_ORIGIN <> 'D'),
       Q7 as (
select /*+ inline parallel(8) */
       EMPLID PERSON_ID, SRC_SYS_ID,
       NATIONAL_ID ITIN
  from CSSTG_OWNER.PS_PERS_NID
 where DATA_ORIGIN <> 'D'
   and COUNTRY = 'USA'
   and NATIONAL_ID_TYPE = 'ITIN'),
       Q8 as (
select /*+ inline parallel(8) */
       EMPLID PERSON_ID, SRC_SYS_ID,
       NATIONAL_ID NTNL_ID,
       CASE WHEN LENGTH (TRIM (NATIONAL_ID)) = 9 THEN 'Valid Length'
            WHEN LENGTH (TRIM (NATIONAL_ID)) < 9 THEN 'Length less than 9'
            WHEN LENGTH (TRIM (NATIONAL_ID)) > 9 THEN 'Length greater than 9'
        END NTNL_ID_ERR_CHK,
       CASE WHEN LENGTH (TRIM (NATIONAL_ID)) = 9
            THEN 'XXX-XX-' || SUBSTR (NATIONAL_ID, 6, 4)
            ELSE NATIONAL_ID
        END MASKED_NTNL_ID
  from CSSTG_OWNER.PS_PERS_NID
 where DATA_ORIGIN <> 'D'
   and COUNTRY = 'USA'
   and NATIONAL_ID_TYPE = 'PR'),
       Q9 as (
select /*+ inline parallel(8) */
       EMPLID PERSON_ID, NAME_TYPE, SRC_SYS_ID,
       EFFDT, EFF_STATUS,
       NAME, NAME_SUFFIX, LAST_NAME, FIRST_NAME, MIDDLE_NAME,
       row_number() over (partition by EMPLID, NAME_TYPE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_NAMES
 where DATA_ORIGIN <> 'D'),
       Q10 as (
select /*+ inline parallel(8) */
       PERSON_ID, SRC_SYS_ID,
       MAX(CASE WHEN NAME_TYPE = 'PRI' THEN NAME        ELSE ('') END) PERSON_NM,
       MAX(CASE WHEN NAME_TYPE = 'PRI' THEN FIRST_NAME  ELSE ('') END) FIRST_NM,
       MAX(CASE WHEN NAME_TYPE = 'PRI' THEN MIDDLE_NAME ELSE ('') END) MIDDLE_NM,
       MAX(CASE WHEN NAME_TYPE = 'PRI' THEN LAST_NAME   ELSE ('') END) LAST_NM,
       MAX(CASE WHEN NAME_TYPE = 'PRI' THEN NAME_SUFFIX ELSE ('') END) SUFFIX,
--       MAX(CASE WHEN NAME_TYPE = 'AK1' THEN NAME        ELSE ('') END) AK1_NAME,
--       MAX(CASE WHEN NAME_TYPE = 'AK1' THEN FIRST_NAME  ELSE ('') END) AK1_FIRST_NM,
--       MAX(CASE WHEN NAME_TYPE = 'AK1' THEN MIDDLE_NAME ELSE ('') END) AK1_MIDDLE_NM,
--       MAX(CASE WHEN NAME_TYPE = 'AK1' THEN LAST_NAME   ELSE ('') END) AK1_LAST_NM,
--       MAX(CASE WHEN NAME_TYPE = 'AK1' THEN NAME_SUFFIX ELSE ('') END) AK1_SUFFIX,
       MAX(CASE WHEN NAME_TYPE = 'PRF' THEN NAME        ELSE ('') END) PREFERRED_NAME
--       MAX(CASE WHEN NAME_TYPE = 'DEG' AND EFF_STATUS <> 'I' THEN NAME        ELSE ('') END) DEG_NAME,
--       MAX(CASE WHEN NAME_TYPE = 'DEG' AND EFF_STATUS <> 'I' THEN FIRST_NAME  ELSE ('') END) DEG_FIRST_NAME,
--       MAX(CASE WHEN NAME_TYPE = 'DEG' AND EFF_STATUS <> 'I' THEN MIDDLE_NAME ELSE ('') END) DEG_MIDDLE_NAME,
--       MAX(CASE WHEN NAME_TYPE = 'DEG' AND EFF_STATUS <> 'I' THEN LAST_NAME   ELSE ('') END) DEG_LAST_NAME,
--       MAX(CASE WHEN NAME_TYPE = 'DEG' AND EFF_STATUS <> 'I' THEN NAME_SUFFIX ELSE ('') END) DEG_SUFFIX
  from Q9
 where Q_ORDER = 1
 group by PERSON_ID, SRC_SYS_ID),
       Q11 as (
select /*+ inline parallel(8) */
       EMPLID PERSON_ID, SRC_SYS_ID,
       EXTERNAL_SYSTEM_ID SEVIS_ID,
       row_number() over (partition by EMPLID, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_EXTERNAL_SYSTEM
 where DATA_ORIGIN <> 'D'
   and EXTERNAL_SYSTEM = 'SEV'
   and length(trim(EXTERNAL_SYSTEM_ID)) <= 11),
       S as (
select /*+ inline parallel(8) */
       Q1.PERSON_ID, Q1.SRC_SYS_ID,
       nvl(Q10.PERSON_NM,'-') PERSON_NM,
       nvl(Q10.FIRST_NM,'-') FIRST_NM,
       CASE WHEN Q10.MIDDLE_NM = '-' THEN '' ELSE Q10.MIDDLE_NM END MIDDLE_NM,
       nvl(Q10.LAST_NM,'-') LAST_NM,
       nvl(Q10.SUFFIX,'-') SUFFIX,
       nvl(Q10.PREFERRED_NAME,'-') PREFERRED_NAME,
--       nvl(Q10.AK1_NAME,'-') AK1_NAME,
--       nvl(Q10.AK1_FIRST_NM,'-') AK1_FIRST_NM,
--       CASE WHEN Q10.AK1_MIDDLE_NM = '-' THEN '' ELSE Q10.AK1_MIDDLE_NM END AK1_MIDDLE_NM,
--       nvl(Q10.AK1_LAST_NM,'-') AK1_LAST_NM,
--       nvl(Q10.AK1_SUFFIX,'-') AK1_SUFFIX,
--       CASE WHEN Q10.DEG_NAME IS NULL
--            THEN nvl(Q10.PERSON_NM,'-')
--            ELSE nvl(Q10.DEG_NAME,'-')
--        END DEG_NAME,
--       CASE WHEN Q10.DEG_NAME IS NULL
--            THEN nvl(Q10.FIRST_NM,'-')
--            ELSE nvl(Q10.DEG_FIRST_NAME,'-')
--        END DEG_FIRST_NAME,
--       CASE WHEN Q10.DEG_NAME IS NULL
--            THEN (CASE WHEN Q10.MIDDLE_NM = '-' THEN '' ELSE Q10.MIDDLE_NM END)
--            ELSE (CASE WHEN Q10.DEG_MIDDLE_NAME = '-' THEN '' ELSE Q10.DEG_MIDDLE_NAME END)
--        END DEG_MIDDLE_NAME,
--       CASE WHEN Q10.DEG_NAME IS NULL
--            THEN nvl(Q10.LAST_NM,'-')
--            ELSE nvl(Q10.DEG_LAST_NAME,'-')
--        END DEG_LAST_NAME,
--       CASE WHEN Q10.DEG_NAME IS NULL
--            THEN nvl(Q10.SUFFIX,'-')
--            ELSE nvl(Q10.DEG_SUFFIX,'-')
--        END DEG_SUFFIX,
       Q1.BIRTH_DT, Q1.BIRTH_PLACE, Q1.BIRTH_STATE, Q1.BIRTH_STATE_LD, Q1.BIRTH_COUNTRY, Q1.BIRTH_COUNTRY_SD, Q1.BIRTH_COUNTRY_LD, Q1.BIRTH_COUNTRY_2CHAR, Q1.BIRTH_COUNTRY_EU_MEMBER,
       Q1.DEATH_DT, Q1.DEATH_FLG, nvl(Q4.DEATH_PLACE,'-') DEATH_PLACE,
       nvl(Q3.EMERG_CNTCT_NM,'-') EMERG_CNTCT_NM, nvl(Q3.EMERG_RELATIONSHIP,'-') EMERG_RELATIONSHIP, nvl(Q3.EMERG_RELATIONSHIP_SD,'-') EMERG_RELATIONSHIP_SD, nvl(Q3.EMERG_RELATIONSHIP_LD,'-') EMERG_RELATIONSHIP_LD,
       nvl(Q3.EMERG_ADDR1_LD,'-') EMERG_ADDR1_LD, nvl(Q3.EMERG_ADDR2_LD,'-') EMERG_ADDR2_LD, nvl(Q3.EMERG_ADDR3_LD,'-') EMERG_ADDR3_LD, nvl(Q3.EMERG_ADDR4_LD,'-') EMERG_ADDR4_LD,
       nvl(Q3.EMERG_CITY_NM,'-') EMERG_CITY_NM, nvl(Q3.EMERG_STATE_CD,'-') EMERG_STATE_CD, nvl(Q3.EMERG_POSTAL_CD,'-') EMERG_POSTAL_CD,
       nvl(Q3.EMERG_CNTRY_CD,'-') EMERG_CNTRY_CD, nvl(Q3.EMERG_COUNTRY_CODE,'-') EMERG_COUNTRY_CODE, nvl(Q3.EMERG_PHONE_NUM,'-') EMERG_PHONE_NUM,
       '7NSPEC' ETHNIC_GRP_FED_CD,   -- Update in another merge SQL
       '7NSPEC' ETHNIC_GRP_ST_CD,    -- Update in another merge SQL
       nvl(Q4.FERPA_FLG,'-') FERPA_FLG,
       nvl(Q5.GENDER_CD,'-') GENDER_CD, nvl(X2.XLATSHORTNAME,'Unknown') GENDER_SD, nvl(X2.XLATLONGNAME,'Unknown') GENDER_LD,
       nvl(Q5.HI_EDU_LVL_CD,'-') HI_EDU_LVL_CD, nvl(X4.XLATSHORTNAME,'-') HI_EDU_LVL_SD, nvl(X4.XLATLONGNAME,'-') HI_EDU_LVL_LD,
       nvl(Q5.MAR_STAT_CD,'-') MAR_STAT_CD, nvl(X3.XLATSHORTNAME,'-') MAR_STAT_SD, nvl(X3.XLATLONGNAME,'-') MAR_STAT_LD,
       case when Q5.MAR_STAT_DT is NULL or Q5.MAR_STAT_DT <= '01-JAN-1901' then NULL else Q5.MAR_STAT_DT end MAR_STAT_DT,
       nvl(Q6.MIL_STAT_CD,'-') MIL_STAT_CD, nvl(X5.XLATSHORTNAME,'-') MIL_STAT_SD, nvl(X5.XLATLONGNAME,'-') MIL_STAT_LD,
       nvl(Q7.ITIN,'-') ITIN,
       nvl(Q8.NTNL_ID,'-') NTNL_ID, nvl(Q8.NTNL_ID_ERR_CHK,'-') NTNL_ID_ERR_CHK, nvl(Q8.MASKED_NTNL_ID,'-') MASKED_NTNL_ID,
       nvl(Q11.SEVIS_ID,'-') SEVIS_ID,
       '-' UNDER_MINORITY_FLAG,    -- Update in another merge SQL   -- Feb 2020
       nvl(Q6.US_WORK_ELIG_IND,'-') US_WORK_ELIG_IND,
       nvl(Q4.VA_BENEFIT,'-') VA_BENEFIT,
       Q1.DATA_ORIGIN
  from Q1
  left outer join Q3
    on Q1.PERSON_ID = Q3.PERSON_ID
   and Q1.SRC_SYS_ID = Q3.SRC_SYS_ID
   and Q3.Q_ORDER = 1
  left outer join Q4
    on Q1.PERSON_ID = Q4.PERSON_ID
   and Q1.SRC_SYS_ID = Q4.SRC_SYS_ID
  left outer join Q5
    on Q1.PERSON_ID = Q5.PERSON_ID
   and Q1.SRC_SYS_ID = Q5.SRC_SYS_ID
   and Q5.Q_ORDER = 1
  left outer join Q6
    on Q1.PERSON_ID = Q6.PERSON_ID
   and Q1.SRC_SYS_ID = Q6.SRC_SYS_ID
   and Q6.Q_ORDER = 1
  left outer join Q7
    on Q1.PERSON_ID = Q7.PERSON_ID
   and Q1.SRC_SYS_ID = Q7.SRC_SYS_ID
  left outer join Q8
    on Q1.PERSON_ID = Q8.PERSON_ID
   and Q1.SRC_SYS_ID = Q8.SRC_SYS_ID
  left outer join Q10
    on Q1.PERSON_ID = Q10.PERSON_ID
   and Q1.SRC_SYS_ID = Q10.SRC_SYS_ID
  left outer join Q11
    on Q1.PERSON_ID = Q11.PERSON_ID
   and Q1.SRC_SYS_ID = Q11.SRC_SYS_ID
   and Q11.Q_ORDER = 1
  left outer join X X2
    on Q5.GENDER_CD = X2.FIELDVALUE
   and Q5.SRC_SYS_ID = X2.SRC_SYS_ID
   and X2.FIELDNAME = 'SEX'
   and X2.X_ORDER = 1
  left outer join X X3
    on Q5.MAR_STAT_CD = X3.FIELDVALUE
   and Q5.SRC_SYS_ID = X3.SRC_SYS_ID
   and X3.FIELDNAME = 'MAR_STATUS'
   and X3.X_ORDER = 1
  left outer join X X4
    on Q5.HI_EDU_LVL_CD = X4.FIELDVALUE
   and Q5.SRC_SYS_ID = X4.SRC_SYS_ID
   and X4.FIELDNAME = 'HIGHEST_EDUC_LVL'
   and X4.X_ORDER = 1
  left outer join X X5
    on Q6.MIL_STAT_CD = X5.FIELDVALUE
   and Q6.SRC_SYS_ID = X5.SRC_SYS_ID
   and X5.FIELDNAME = 'MILITARY_STATUS'
   and X5.X_ORDER = 1)
--select nvl(D.PERSON_SID, max(D.PERSON_SID) over (partition by 1) +
--       row_number() over (partition by 1 order by D.PERSON_SID nulls first)) PERSON_SID,
select nvl(D.PERSON_SID,
          (select nvl(max(PERSON_SID),0) from CSMRT_OWNER.PS_D_PERSON where PERSON_SID <> 2147483646) +
                  row_number() over (partition by 1 order by D.PERSON_SID nulls first)) PERSON_SID,
       nvl(D.PERSON_ID, S.PERSON_ID) PERSON_ID,
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,
       decode(D.PERSON_NM, S.PERSON_NM, D.PERSON_NM, S.PERSON_NM) PERSON_NM,
       decode(D.FIRST_NM, S.FIRST_NM, D.FIRST_NM, S.FIRST_NM) FIRST_NM,
       decode(D.MIDDLE_NM, S.MIDDLE_NM, D.MIDDLE_NM, S.MIDDLE_NM) MIDDLE_NM,
       decode(D.LAST_NM, S.LAST_NM, D.LAST_NM, S.LAST_NM) LAST_NM,
       decode(D.SUFFIX, S.SUFFIX, D.SUFFIX, S.SUFFIX) SUFFIX,
       decode(D.PREFERRED_NAME, S.PREFERRED_NAME, D.PREFERRED_NAME, S.PREFERRED_NAME) PREFERRED_NAME,
       decode(D.BIRTH_DT, S.BIRTH_DT, D.BIRTH_DT, S.BIRTH_DT) BIRTH_DT,
       decode(D.BIRTH_PLACE, S.BIRTH_PLACE, D.BIRTH_PLACE, S.BIRTH_PLACE) BIRTH_PLACE,
       decode(D.BIRTH_STATE, S.BIRTH_STATE, D.BIRTH_STATE, S.BIRTH_STATE) BIRTH_STATE,
       decode(D.BIRTH_STATE_LD, S.BIRTH_STATE_LD, D.BIRTH_STATE_LD, S.BIRTH_STATE_LD) BIRTH_STATE_LD,
       decode(D.BIRTH_COUNTRY, S.BIRTH_COUNTRY, D.BIRTH_COUNTRY, S.BIRTH_COUNTRY) BIRTH_COUNTRY,
       decode(D.BIRTH_COUNTRY_SD, S.BIRTH_COUNTRY_SD, D.BIRTH_COUNTRY_SD, S.BIRTH_COUNTRY_SD) BIRTH_COUNTRY_SD,
       decode(D.BIRTH_COUNTRY_LD, S.BIRTH_COUNTRY_LD, D.BIRTH_COUNTRY_LD, S.BIRTH_COUNTRY_LD) BIRTH_COUNTRY_LD,
       decode(D.BIRTH_COUNTRY_2CHAR, S.BIRTH_COUNTRY_2CHAR, D.BIRTH_COUNTRY_2CHAR, S.BIRTH_COUNTRY_2CHAR) BIRTH_COUNTRY_2CHAR,
       decode(D.BIRTH_COUNTRY_EU_MEMBER, S.BIRTH_COUNTRY_EU_MEMBER, D.BIRTH_COUNTRY_EU_MEMBER, S.BIRTH_COUNTRY_EU_MEMBER) BIRTH_COUNTRY_EU_MEMBER,
       decode(D.DEATH_DT, S.DEATH_DT, D.DEATH_DT, S.DEATH_DT) DEATH_DT,
       decode(D.DEATH_FLG, S.DEATH_FLG, D.DEATH_FLG, S.DEATH_FLG) DEATH_FLG,
       decode(D.DEATH_PLACE, S.DEATH_PLACE, D.DEATH_PLACE, S.DEATH_PLACE) DEATH_PLACE,
       decode(D.EMERG_CNTCT_NM, S.EMERG_CNTCT_NM, D.EMERG_CNTCT_NM, S.EMERG_CNTCT_NM) EMERG_CNTCT_NM,
       decode(D.EMERG_RELATIONSHIP, S.EMERG_RELATIONSHIP, D.EMERG_RELATIONSHIP, S.EMERG_RELATIONSHIP) EMERG_RELATIONSHIP,
       decode(D.EMERG_RELATIONSHIP_SD, S.EMERG_RELATIONSHIP_SD, D.EMERG_RELATIONSHIP_SD, S.EMERG_RELATIONSHIP_SD) EMERG_RELATIONSHIP_SD,
       decode(D.EMERG_RELATIONSHIP_LD, S.EMERG_RELATIONSHIP_LD, D.EMERG_RELATIONSHIP_LD, S.EMERG_RELATIONSHIP_LD) EMERG_RELATIONSHIP_LD,
       decode(D.EMERG_ADDR1_LD, S.EMERG_ADDR1_LD, D.EMERG_ADDR1_LD, S.EMERG_ADDR1_LD) EMERG_ADDR1_LD,
       decode(D.EMERG_ADDR2_LD, S.EMERG_ADDR2_LD, D.EMERG_ADDR2_LD, S.EMERG_ADDR2_LD) EMERG_ADDR2_LD,
       decode(D.EMERG_ADDR3_LD, S.EMERG_ADDR3_LD, D.EMERG_ADDR3_LD, S.EMERG_ADDR3_LD) EMERG_ADDR3_LD,
       decode(D.EMERG_ADDR4_LD, S.EMERG_ADDR4_LD, D.EMERG_ADDR4_LD, S.EMERG_ADDR4_LD) EMERG_ADDR4_LD,
       decode(D.EMERG_CITY_NM, S.EMERG_CITY_NM, D.EMERG_CITY_NM, S.EMERG_CITY_NM) EMERG_CITY_NM,
       decode(D.EMERG_STATE_CD, S.EMERG_STATE_CD, D.EMERG_STATE_CD, S.EMERG_STATE_CD) EMERG_STATE_CD,
       decode(D.EMERG_POSTAL_CD, S.EMERG_POSTAL_CD, D.EMERG_POSTAL_CD, S.EMERG_POSTAL_CD) EMERG_POSTAL_CD,
       decode(D.EMERG_CNTRY_CD, S.EMERG_CNTRY_CD, D.EMERG_CNTRY_CD, S.EMERG_CNTRY_CD) EMERG_CNTRY_CD,
       decode(D.EMERG_COUNTRY_CODE, S.EMERG_COUNTRY_CODE, D.EMERG_COUNTRY_CODE, S.EMERG_COUNTRY_CODE) EMERG_COUNTRY_CODE,
       decode(D.EMERG_PHONE_NUM, S.EMERG_PHONE_NUM, D.EMERG_PHONE_NUM, S.EMERG_PHONE_NUM) EMERG_PHONE_NUM,
       decode(D.ETHNIC_GRP_FED_CD, S.ETHNIC_GRP_FED_CD, D.ETHNIC_GRP_FED_CD, S.ETHNIC_GRP_FED_CD) ETHNIC_GRP_FED_CD,
       decode(D.ETHNIC_GRP_ST_CD, S.ETHNIC_GRP_ST_CD, D.ETHNIC_GRP_ST_CD, S.ETHNIC_GRP_ST_CD) ETHNIC_GRP_ST_CD,
       decode(D.FERPA_FLG, S.FERPA_FLG, D.FERPA_FLG, S.FERPA_FLG) FERPA_FLG,
       decode(D.GENDER_CD, S.GENDER_CD, D.GENDER_CD, S.GENDER_CD) GENDER_CD,
       decode(D.GENDER_SD, S.GENDER_SD, D.GENDER_SD, S.GENDER_SD) GENDER_SD,
       decode(D.GENDER_LD, S.GENDER_LD, D.GENDER_LD, S.GENDER_LD) GENDER_LD,
       decode(D.HI_EDU_LVL_CD, S.HI_EDU_LVL_CD, D.HI_EDU_LVL_CD, S.HI_EDU_LVL_CD) HI_EDU_LVL_CD,
       decode(D.HI_EDU_LVL_SD, S.HI_EDU_LVL_SD, D.HI_EDU_LVL_SD, S.HI_EDU_LVL_SD) HI_EDU_LVL_SD,
       decode(D.HI_EDU_LVL_LD, S.HI_EDU_LVL_LD, D.HI_EDU_LVL_LD, S.HI_EDU_LVL_LD) HI_EDU_LVL_LD,
       decode(D.MAR_STAT_CD, S.MAR_STAT_CD, D.MAR_STAT_CD, S.MAR_STAT_CD) MAR_STAT_CD,
       decode(D.MAR_STAT_SD, S.MAR_STAT_SD, D.MAR_STAT_SD, S.MAR_STAT_SD) MAR_STAT_SD,
       decode(D.MAR_STAT_LD, S.MAR_STAT_LD, D.MAR_STAT_LD, S.MAR_STAT_LD) MAR_STAT_LD,
       decode(D.MAR_STAT_DT, S.MAR_STAT_DT, D.MAR_STAT_DT, S.MAR_STAT_DT) MAR_STAT_DT,
       decode(D.MIL_STAT_CD, S.MIL_STAT_CD, D.MIL_STAT_CD, S.MIL_STAT_CD) MIL_STAT_CD,
       decode(D.MIL_STAT_SD, S.MIL_STAT_SD, D.MIL_STAT_SD, S.MIL_STAT_SD) MIL_STAT_SD,
       decode(D.MIL_STAT_LD, S.MIL_STAT_LD, D.MIL_STAT_LD, S.MIL_STAT_LD) MIL_STAT_LD,
       decode(D.ITIN, S.ITIN, D.ITIN, S.ITIN) ITIN,
       decode(D.NTNL_ID, S.NTNL_ID, D.NTNL_ID, S.NTNL_ID) NTNL_ID,
       decode(D.NTNL_ID_ERR_CHK, S.NTNL_ID_ERR_CHK, D.NTNL_ID_ERR_CHK, S.NTNL_ID_ERR_CHK) NTNL_ID_ERR_CHK,
       decode(D.MASKED_NTNL_ID, S.MASKED_NTNL_ID, D.MASKED_NTNL_ID, S.MASKED_NTNL_ID) MASKED_NTNL_ID,
       decode(D.SEVIS_ID, S.SEVIS_ID, D.SEVIS_ID, S.SEVIS_ID) SEVIS_ID,
       decode(D.UNDER_MINORITY_FLAG, S.UNDER_MINORITY_FLAG, D.UNDER_MINORITY_FLAG, S.UNDER_MINORITY_FLAG) UNDER_MINORITY_FLAG,      -- Feb 2020
       decode(D.US_WORK_ELIG_IND, S.US_WORK_ELIG_IND, D.US_WORK_ELIG_IND, S.US_WORK_ELIG_IND) US_WORK_ELIG_IND,
       decode(D.VA_BENEFIT, S.VA_BENEFIT, D.VA_BENEFIT, S.VA_BENEFIT) VA_BENEFIT,
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,
      nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM
  from S
  left outer join CSMRT_OWNER.PS_D_PERSON D
    on D.PERSON_SID <> 2147483646
   and D.PERSON_ID = S.PERSON_ID
   and D.SRC_SYS_ID = S.SRC_SYS_ID
) S
    on  (T.PERSON_ID = S.PERSON_ID
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)
 when matched then update set
       T.PERSON_NM = S.PERSON_NM,
       T.FIRST_NM = S.FIRST_NM,
       T.MIDDLE_NM = S.MIDDLE_NM,
       T.LAST_NM = S.LAST_NM,
       T.SUFFIX = S.SUFFIX,
       T.PREFERRED_NAME = S.PREFERRED_NAME,
       T.BIRTH_DT = S.BIRTH_DT,
       T.BIRTH_PLACE = S.BIRTH_PLACE,
       T.BIRTH_STATE = S.BIRTH_STATE,
       T.BIRTH_STATE_LD = S.BIRTH_STATE_LD,
       T.BIRTH_COUNTRY = S.BIRTH_COUNTRY,
       T.BIRTH_COUNTRY_SD = S.BIRTH_COUNTRY_SD,
       T.BIRTH_COUNTRY_LD = S.BIRTH_COUNTRY_LD,
       T.BIRTH_COUNTRY_2CHAR = S.BIRTH_COUNTRY_2CHAR,
       T.BIRTH_COUNTRY_EU_MEMBER = S.BIRTH_COUNTRY_EU_MEMBER,
       T.DEATH_DT = S.DEATH_DT,
       T.DEATH_FLG = S.DEATH_FLG,
       T.DEATH_PLACE = S.DEATH_PLACE,
       T.EMERG_CNTCT_NM = S.EMERG_CNTCT_NM,
       T.EMERG_RELATIONSHIP = S.EMERG_RELATIONSHIP,
       T.EMERG_RELATIONSHIP_SD = S.EMERG_RELATIONSHIP_SD,
       T.EMERG_RELATIONSHIP_LD = S.EMERG_RELATIONSHIP_LD,
       T.EMERG_ADDR1_LD = S.EMERG_ADDR1_LD,
       T.EMERG_ADDR2_LD = S.EMERG_ADDR2_LD,
       T.EMERG_ADDR3_LD = S.EMERG_ADDR3_LD,
       T.EMERG_ADDR4_LD = S.EMERG_ADDR4_LD,
       T.EMERG_CITY_NM = S.EMERG_CITY_NM,
       T.EMERG_STATE_CD = S.EMERG_STATE_CD,
       T.EMERG_POSTAL_CD = S.EMERG_POSTAL_CD,
       T.EMERG_CNTRY_CD = S.EMERG_CNTRY_CD,
       T.EMERG_COUNTRY_CODE = S.EMERG_COUNTRY_CODE,
       T.EMERG_PHONE_NUM = S.EMERG_PHONE_NUM,
--       T.ETHNIC_GRP_FED_CD = S.ETHNIC_GRP_FED_CD,
--       T.ETHNIC_GRP_ST_CD = S.ETHNIC_GRP_ST_CD,
       T.FERPA_FLG = S.FERPA_FLG,
       T.GENDER_CD = S.GENDER_CD,
       T.GENDER_SD = S.GENDER_SD,
       T.GENDER_LD = S.GENDER_LD,
       T.HI_EDU_LVL_CD = S.HI_EDU_LVL_CD,
       T.HI_EDU_LVL_SD = S.HI_EDU_LVL_SD,
       T.HI_EDU_LVL_LD = S.HI_EDU_LVL_LD,
       T.MAR_STAT_CD = S.MAR_STAT_CD,
       T.MAR_STAT_SD = S.MAR_STAT_SD,
       T.MAR_STAT_LD = S.MAR_STAT_LD,
       T.MAR_STAT_DT = S.MAR_STAT_DT,
       T.MIL_STAT_CD = S.MIL_STAT_CD,
       T.MIL_STAT_SD = S.MIL_STAT_SD,
       T.MIL_STAT_LD = S.MIL_STAT_LD,
       T.ITIN = S.ITIN,
       T.NTNL_ID = S.NTNL_ID,
       T.NTNL_ID_ERR_CHK = S.NTNL_ID_ERR_CHK,
       T.MASKED_NTNL_ID = S.MASKED_NTNL_ID,
       T.SEVIS_ID = S.SEVIS_ID,
       T.US_WORK_ELIG_IND = S.US_WORK_ELIG_IND,
       T.VA_BENEFIT = S.VA_BENEFIT,
       T.DATA_ORIGIN = S.DATA_ORIGIN,
       T.LASTUPD_EW_DTTM = SYSDATE
 where
       decode(T.PERSON_NM,S.PERSON_NM,0,1) = 1 or
       decode(T.FIRST_NM,S.FIRST_NM,0,1) = 1 or
       decode(T.MIDDLE_NM,S.MIDDLE_NM,0,1) = 1 or
       decode(T.LAST_NM,S.LAST_NM,0,1) = 1 or
       decode(T.SUFFIX,S.SUFFIX,0,1) = 1 or
       decode(T.PREFERRED_NAME,S.PREFERRED_NAME,0,1) = 1 or
       decode(T.BIRTH_DT,S.BIRTH_DT,0,1) = 1 or
       decode(T.BIRTH_PLACE,S.BIRTH_PLACE,0,1) = 1 or
       decode(T.BIRTH_STATE,S.BIRTH_STATE,0,1) = 1 or
       decode(T.BIRTH_STATE_LD,S.BIRTH_STATE_LD,0,1) = 1 or
       decode(T.BIRTH_COUNTRY,S.BIRTH_COUNTRY,0,1) = 1 or
       decode(T.BIRTH_COUNTRY_SD,S.BIRTH_COUNTRY_SD,0,1) = 1 or
       decode(T.BIRTH_COUNTRY_LD,S.BIRTH_COUNTRY_LD,0,1) = 1 or
       decode(T.BIRTH_COUNTRY_2CHAR,S.BIRTH_COUNTRY_2CHAR,0,1) = 1 or
       decode(T.BIRTH_COUNTRY_EU_MEMBER,S.BIRTH_COUNTRY_EU_MEMBER,0,1) = 1 or
       decode(T.DEATH_DT,S.DEATH_DT,0,1) = 1 or
       decode(T.DEATH_FLG,S.DEATH_FLG,0,1) = 1 or
       decode(T.DEATH_PLACE,S.DEATH_PLACE,0,1) = 1 or
       decode(T.EMERG_CNTCT_NM,S.EMERG_CNTCT_NM,0,1) = 1 or
       decode(T.EMERG_RELATIONSHIP,S.EMERG_RELATIONSHIP,0,1) = 1 or
       decode(T.EMERG_RELATIONSHIP_SD,S.EMERG_RELATIONSHIP_SD,0,1) = 1 or
       decode(T.EMERG_RELATIONSHIP_LD,S.EMERG_RELATIONSHIP_LD,0,1) = 1 or
       decode(T.EMERG_ADDR1_LD,S.EMERG_ADDR1_LD,0,1) = 1 or
       decode(T.EMERG_ADDR2_LD,S.EMERG_ADDR2_LD,0,1) = 1 or
       decode(T.EMERG_ADDR3_LD,S.EMERG_ADDR3_LD,0,1) = 1 or
       decode(T.EMERG_ADDR4_LD,S.EMERG_ADDR4_LD,0,1) = 1 or
       decode(T.EMERG_CITY_NM,S.EMERG_CITY_NM,0,1) = 1 or
       decode(T.EMERG_STATE_CD,S.EMERG_STATE_CD,0,1) = 1 or
       decode(T.EMERG_POSTAL_CD,S.EMERG_POSTAL_CD,0,1) = 1 or
       decode(T.EMERG_CNTRY_CD,S.EMERG_CNTRY_CD,0,1) = 1 or
       decode(T.EMERG_COUNTRY_CODE,S.EMERG_COUNTRY_CODE,0,1) = 1 or
       decode(T.EMERG_PHONE_NUM,S.EMERG_PHONE_NUM,0,1) = 1 or
--       decode(T.ETHNIC_GRP_FED_CD,S.ETHNIC_GRP_FED_CD,0,1) = 1 or
--       decode(T.ETHNIC_GRP_ST_CD,S.ETHNIC_GRP_ST_CD,0,1) = 1 or
       decode(T.FERPA_FLG,S.FERPA_FLG,0,1) = 1 or
       decode(T.GENDER_CD,S.GENDER_CD,0,1) = 1 or
       decode(T.GENDER_SD,S.GENDER_SD,0,1) = 1 or
       decode(T.GENDER_LD,S.GENDER_LD,0,1) = 1 or
       decode(T.HI_EDU_LVL_CD,S.HI_EDU_LVL_CD,0,1) = 1 or
       decode(T.HI_EDU_LVL_SD,S.HI_EDU_LVL_SD,0,1) = 1 or
       decode(T.HI_EDU_LVL_LD,S.HI_EDU_LVL_LD,0,1) = 1 or
       decode(T.MAR_STAT_CD,S.MAR_STAT_CD,0,1) = 1 or
       decode(T.MAR_STAT_SD,S.MAR_STAT_SD,0,1) = 1 or
       decode(T.MAR_STAT_LD,S.MAR_STAT_LD,0,1) = 1 or
       decode(T.MAR_STAT_DT,S.MAR_STAT_DT,0,1) = 1 or
       decode(T.MIL_STAT_CD,S.MIL_STAT_CD,0,1) = 1 or
       decode(T.MIL_STAT_SD,S.MIL_STAT_SD,0,1) = 1 or
       decode(T.MIL_STAT_LD,S.MIL_STAT_LD,0,1) = 1 or
       decode(T.ITIN,S.ITIN,0,1) = 1 or
       decode(T.NTNL_ID,S.NTNL_ID,0,1) = 1 or
       decode(T.NTNL_ID_ERR_CHK,S.NTNL_ID_ERR_CHK,0,1) = 1 or
       decode(T.MASKED_NTNL_ID,S.MASKED_NTNL_ID,0,1) = 1 or
       decode(T.SEVIS_ID,S.SEVIS_ID,0,1) = 1 or
       decode(T.US_WORK_ELIG_IND,S.US_WORK_ELIG_IND,0,1) = 1 or
       decode(T.VA_BENEFIT,S.VA_BENEFIT,0,1) = 1 or
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1
  when not matched then
insert (
       T.PERSON_SID,
       T.PERSON_ID,
       T.SRC_SYS_ID,
       T.PERSON_NM,
       T.FIRST_NM,
       T.MIDDLE_NM,
       T.LAST_NM,
       T.SUFFIX,
       T.PREFERRED_NAME,
       T.BIRTH_DT,
       T.BIRTH_PLACE,
       T.BIRTH_STATE,
       T.BIRTH_STATE_LD,
       T.BIRTH_COUNTRY,
       T.BIRTH_COUNTRY_SD,
       T.BIRTH_COUNTRY_LD,
       T.BIRTH_COUNTRY_2CHAR,
       T.BIRTH_COUNTRY_EU_MEMBER,
       T.DEATH_DT,
       T.DEATH_FLG,
       T.DEATH_PLACE,
       T.EMERG_CNTCT_NM,
       T.EMERG_RELATIONSHIP,
       T.EMERG_RELATIONSHIP_SD,
       T.EMERG_RELATIONSHIP_LD,
       T.EMERG_ADDR1_LD,
       T.EMERG_ADDR2_LD,
       T.EMERG_ADDR3_LD,
       T.EMERG_ADDR4_LD,
       T.EMERG_CITY_NM,
       T.EMERG_STATE_CD,
       T.EMERG_POSTAL_CD,
       T.EMERG_CNTRY_CD,
       T.EMERG_COUNTRY_CODE,
       T.EMERG_PHONE_NUM,
       T.ETHNIC_GRP_FED_CD,
       T.ETHNIC_GRP_ST_CD,
       T.FERPA_FLG,
       T.GENDER_CD,
       T.GENDER_SD,
       T.GENDER_LD,
       T.HI_EDU_LVL_CD,
       T.HI_EDU_LVL_SD,
       T.HI_EDU_LVL_LD,
       T.MAR_STAT_CD,
       T.MAR_STAT_SD,
       T.MAR_STAT_LD,
       T.MAR_STAT_DT,
       T.MIL_STAT_CD,
       T.MIL_STAT_SD,
       T.MIL_STAT_LD,
       T.ITIN,
       T.NTNL_ID,
       T.NTNL_ID_ERR_CHK,
       T.MASKED_NTNL_ID,
       T.SEVIS_ID,
       T.UNDER_MINORITY_FLAG,      -- Feb 2020
       T.US_WORK_ELIG_IND,
       T.VA_BENEFIT,
       T.DATA_ORIGIN,
       T.CREATED_EW_DTTM,
       T.LASTUPD_EW_DTTM)
values (
       S.PERSON_SID,
       S.PERSON_ID,
       S.SRC_SYS_ID,
       S.PERSON_NM,
       S.FIRST_NM,
       S.MIDDLE_NM,
       S.LAST_NM,
       S.SUFFIX,
       S.PREFERRED_NAME,
       S.BIRTH_DT,
       S.BIRTH_PLACE,
       S.BIRTH_STATE,
       S.BIRTH_STATE_LD,
       S.BIRTH_COUNTRY,
       S.BIRTH_COUNTRY_SD,
       S.BIRTH_COUNTRY_LD,
       S.BIRTH_COUNTRY_2CHAR,
       S.BIRTH_COUNTRY_EU_MEMBER,
       S.DEATH_DT,
       S.DEATH_FLG,
       S.DEATH_PLACE,
       S.EMERG_CNTCT_NM,
       S.EMERG_RELATIONSHIP,
       S.EMERG_RELATIONSHIP_SD,
       S.EMERG_RELATIONSHIP_LD,
       S.EMERG_ADDR1_LD,
       S.EMERG_ADDR2_LD,
       S.EMERG_ADDR3_LD,
       S.EMERG_ADDR4_LD,
       S.EMERG_CITY_NM,
       S.EMERG_STATE_CD,
       S.EMERG_POSTAL_CD,
       S.EMERG_CNTRY_CD,
       S.EMERG_COUNTRY_CODE,
       S.EMERG_PHONE_NUM,
       S.ETHNIC_GRP_FED_CD,
       S.ETHNIC_GRP_ST_CD,
       S.FERPA_FLG,
       S.GENDER_CD,
       S.GENDER_SD,
       S.GENDER_LD,
       S.HI_EDU_LVL_CD,
       S.HI_EDU_LVL_SD,
       S.HI_EDU_LVL_LD,
       S.MAR_STAT_CD,
       S.MAR_STAT_SD,
       S.MAR_STAT_LD,
       S.MAR_STAT_DT,
       S.MIL_STAT_CD,
       S.MIL_STAT_SD,
       S.MIL_STAT_LD,
       S.ITIN,
       S.NTNL_ID,
       S.NTNL_ID_ERR_CHK,
       S.MASKED_NTNL_ID,
       S.SEVIS_ID,
       S.UNDER_MINORITY_FLAG,   -- Feb 2020
       S.US_WORK_ELIG_IND,
       S.VA_BENEFIT,
       S.DATA_ORIGIN,
       SYSDATE,
       SYSDATE);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_PERSON rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_PERSON',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

	  -- ETHNIC_GRP_FED_CD, ETHNIC_GRP_ST_CD, and UNDER_MINORITY_FLAG updated with a below merge SQL!!!

merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_PERSON T
using (
with Q11 as (
select /*+ inline parallel(8) */
       EMPLID PERSON_ID, SRC_SYS_ID,
       CITIZENSHIP_STATUS
  from CSSTG_OWNER.PS_CITIZENSHIP
 where DEPENDENT_ID = '-'
   and COUNTRY = 'USA'
   and DATA_ORIGIN <> 'D'),
       Q12 as (
select /*+ inline parallel(8) */
       EMPLID PERSON_ID, VISA_PERMIT_TYPE, SRC_SYS_ID,
       row_number() over (partition by EMPLID, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc,
                                                         (case when VISA_PERMIT_TYPE in ('AP','AR1','ASY','RFG') then 0 else 1 end)) Q_ORDER
  from CSSTG_OWNER.PS_VISA_PMT_DATA
 where COUNTRY = 'USA'
   and DATA_ORIGIN <> 'D'),
       Q13 as (
select /*+ inline parallel(8) */
       ETHNIC_GRP_CD, SRC_SYS_ID,
       ETHNIC_GROUP,
       CASE WHEN ETHNIC_GROUP NOT IN ('A','B','E','F','G','H','I','J','N') THEN 'Y' ELSE 'N' END NEW_FLAG,
       row_number() over (partition by SETID, ETHNIC_GRP_CD, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_ETHNIC_GRP_TBL
 where SETID = 'USA'
   and DATA_ORIGIN <> 'D'
   and not (ETHNIC_GROUP = '-' and ETHNIC_GRP_CD not in ('0MULTI','8VERDEAN'))        -- Eliminate invalid ethnic group rollups
       ),
       Q14 as (
select /*+ inline parallel(8) */
      D.EMPLID PERSON_ID,
      (CASE WHEN D.ETHNIC_GRP_CD = '7' THEN 'B' ELSE D.ETHNIC_GRP_CD END ) ETHNIC_GRP_CD,     -- VERDEAN DATA FIX
      D.SRC_SYS_ID,
      Q13.ETHNIC_GROUP,
      Q13.NEW_FLAG,
      SUM(CASE WHEN Q13.NEW_FLAG = 'N' THEN 1 ELSE 0 END)
          OVER (PARTITION BY D.EMPLID, D.SRC_SYS_ID ORDER BY D.ETHNIC_GRP_CD
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) OLD_CNT,
      SUM(CASE WHEN Q13.NEW_FLAG = 'Y' THEN 1 ELSE 0 END)
          OVER (PARTITION BY D.EMPLID, D.SRC_SYS_ID ORDER BY D.ETHNIC_GRP_CD
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) NEW_CNT,
      SUM(CASE WHEN Q13.ETHNIC_GROUP IN ('6') THEN 1 ELSE 0 END)
          OVER (PARTITION BY D.EMPLID, D.SRC_SYS_ID ORDER BY D.ETHNIC_GRP_CD
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) NSPEC_CNT,
      SUM(CASE WHEN D.ETHNIC_GRP_CD IN ('VERDEAN','7') THEN 1 ELSE 0 END)
          OVER (PARTITION BY D.EMPLID, D.SRC_SYS_ID ORDER BY D.ETHNIC_GRP_CD
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) VERDEAN_CNT,
      SUM(CASE WHEN D.ETHNIC_GRP_CD IN ('VERDEAN') THEN 1 ELSE 0 END)
          OVER (PARTITION BY D.EMPLID, D.SRC_SYS_ID ORDER BY D.ETHNIC_GRP_CD
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) NEW_VERD_CNT,
      SUM(CASE WHEN Q13.ETHNIC_GROUP IN ('1','2','3','4','5','7','A','B','E','F','H','I')
               THEN 1
               ELSE 0 END)
          OVER (PARTITION BY D.EMPLID, D.SRC_SYS_ID ORDER BY D.ETHNIC_GRP_CD
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) PERSON_CNT,     -- Mar 2020
      SUM(CASE WHEN Q13.ETHNIC_GROUP IN ('1','4','A','E') THEN 1 ELSE 0 END)
          OVER (PARTITION BY D.EMPLID, D.SRC_SYS_ID ORDER BY D.ETHNIC_GRP_CD
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) WASIAN_CNT,     -- Mar 2020
      CASE WHEN Q13.ETHNIC_GROUP IN ('H','3') THEN 1 ELSE 0 END  HISP_CNT,
      CASE WHEN Q13.ETHNIC_GROUP IN ('J') THEN 1 ELSE 0 END  NR_CNT
  from Q13
  join CSSTG_OWNER.PS_ETHNICITY_DTL D
    on Q13.ETHNIC_GRP_CD = D.ETHNIC_GRP_CD
   and Q13.SRC_SYS_ID = D.SRC_SYS_ID
   and Q13.Q_ORDER = 1
   and D.REG_REGION = 'USA'
   and D.DATA_ORIGIN <> 'D'
   and D.ETHNIC_GRP_CD <> '9NONRES'
   ),
       Q15 as (
select /*+ inline parallel(8) */ distinct
       Q14.PERSON_ID, Q14.SRC_SYS_ID,
       decode(Q13.NEW_FLAG,'N', decode(Q14.ETHNIC_GRP_CD,'B','2',Q14.ETHNIC_GRP_CD),Q14.ETHNIC_GROUP) as ETHNIC_GROUP,
       (case when Q13.NEW_FLAG = 'Y' and Q14.NEW_CNT > 0 then Q14.NEW_VERD_CNT else Q14.VERDEAN_CNT end) VERDEAN_CNT,
       (case when Q14.WASIAN_CNT > 0 and Q14.PERSON_CNT <= Q14.WASIAN_CNT then 'Y' else 'N' end) WASIAN_ONLY_FLAG,   -- Mar 2020
       Q14.HISP_CNT, Q14.NR_CNT
  from Q13, Q14
 where Q13.ETHNIC_GROUP = Q14.ETHNIC_GROUP
   and Q13.SRC_SYS_ID = Q14.SRC_SYS_ID
   and not (Q13.ETHNIC_GROUP in ('6','G','N') and (Q14.NEW_CNT > 1 or Q14.OLD_CNT > 0))  -- Use NSPEC only when no other new codes
   and (Q13.NEW_FLAG = 'Y' or (NEW_CNT - NSPEC_CNT = 0))
       ),
       Q16 as (
select /*+ inline parallel(8) */ distinct
      Q15.PERSON_ID, Q15.SRC_SYS_ID,
      decode(Q13.ETHNIC_GRP_CD,'8DNR','7NSPEC',Q13.ETHNIC_GRP_CD) ETHNIC_GRP_CD,
      Q15.VERDEAN_CNT, Q15.WASIAN_ONLY_FLAG,    -- Feb 2020
      Q15.HISP_CNT, Q15.NR_CNT
  from Q13, Q15
 where Q13.ETHNIC_GROUP = Q15.ETHNIC_GROUP
   and Q13.SRC_SYS_ID = Q15.SRC_SYS_ID
   and substr(Q13.ETHNIC_GRP_CD,1,1) between '0' and '9'
   and length(trim(Q13.ETHNIC_GRP_CD)) > 1
   and Q13.ETHNIC_GROUP <> '-'),
       Q17 as (
select /*+ inline parallel(8) */
       Q16.PERSON_ID, Q16.SRC_SYS_ID,
       max(Q16.ETHNIC_GRP_CD) ETHNIC_GRP_CD,
       max(Q16.HISP_CNT) HISP_CNT,
       max(Q16.NR_CNT) NR_CNT,
       max(Q16.VERDEAN_CNT) VERDEAN_CNT,
       min(Q16.WASIAN_ONLY_FLAG) WASIAN_ONLY_FLAG,      -- Feb 2020
       count(*) EMPLID_CNT
  from Q16
 group by Q16.PERSON_ID, Q16.SRC_SYS_ID),
       Q18 as (
select /*+ inline parallel(8) */
       P.EMPLID PERSON_ID, P.SRC_SYS_ID,
       nvl(Q17.ETHNIC_GRP_CD,'-') ETHNIC_GRP_CD,
       nvl(Q17.HISP_CNT,0) HISP_CNT,
       nvl(Q17.NR_CNT,0) NR_CNT,
       nvl(Q17.VERDEAN_CNT,0) VERDEAN_CNT,
       nvl(Q17.WASIAN_ONLY_FLAG,'-') WASIAN_ONLY_FLAG,    -- Feb 2020
       nvl(Q17.EMPLID_CNT,1) EMPLID_CNT,
       case when Q11.CITIZENSHIP_STATUS in ('1','2','3')
            then '1'
            when Q12.VISA_PERMIT_TYPE in ('AP','AR1','ASY','RFG')
            then '1'
            else '0'
        end CIT_IND,
       case when Q12.VISA_PERMIT_TYPE is NULL
            then '0'
            else '1'
        end VISA_IND
  from CSSTG_OWNER.PS_PERSON P
  left outer join Q17
    on P.EMPLID = Q17.PERSON_ID
   and P.SRC_SYS_ID = Q17.SRC_SYS_ID
  left outer join Q11
    on P.EMPLID = Q11.PERSON_ID
   and P.SRC_SYS_ID = Q11.SRC_SYS_ID
  left outer join Q12
    on P.EMPLID = Q12.PERSON_ID
   and P.SRC_SYS_ID = Q12.SRC_SYS_ID
   and Q12.Q_ORDER = 1),
       Q19 as (
select /*+ inline parallel(8) */
       Q18.PERSON_ID, Q18.SRC_SYS_ID,
       Q18.ETHNIC_GRP_CD,
       Q18.HISP_CNT, Q18.NR_CNT,
       Q18.VERDEAN_CNT, Q18.WASIAN_ONLY_FLAG,    -- Feb 2020
       Q18.EMPLID_CNT,
       Q18.CIT_IND, Q18.VISA_IND,
       case when Q18.CIT_IND = '1' and Q18.HISP_CNT >= 1
            then '3HISPLAT'
            when Q18.CIT_IND = '1' and Q18.EMPLID_CNT > 1
            then '0MULTI'
            when Q18.CIT_IND = '1' and Q18.ETHNIC_GRP_CD > '0MULTI' and Q18.ETHNIC_GRP_CD <> '9NONRES'
            then Q18.ETHNIC_GRP_CD
            when Q18.CIT_IND <> '1' and (Q18.VISA_IND = '1' or Q18.NR_CNT >= 1)
            then '9NONRES'
            else '7NSPEC'
        end ETHNIC_GRP_FED_CD
  from Q18)
select /*+ inline parallel(8) */
       Q19.PERSON_ID, Q19.SRC_SYS_ID,
       case when Q19.ETHNIC_GRP_FED_CD = '-'
            then '7NSPEC'
            else Q19.ETHNIC_GRP_FED_CD
        end ETHNIC_GRP_FED_CD,
       case when Q19.ETHNIC_GRP_FED_CD = '-'
            then '7NSPEC'
            when Q19.ETHNIC_GRP_FED_CD = '2AFRAM' and Q19.VERDEAN_CNT > 0
            then '8VERDEAN'
            else Q19.ETHNIC_GRP_FED_CD
        end ETHNIC_GRP_ST_CD,
       (case when Q19.ETHNIC_GRP_FED_CD in ('2AFRAM','3HISPLAT','5HAWPAC','6INDALK') then 'Y'
             when Q19.ETHNIC_GRP_FED_CD = '0MULTI' and Q19.WASIAN_ONLY_FLAG <> 'Y' then 'Y'
             else 'N'
         end) UNDER_MINORITY_FLAG    -- Feb 2020
  from Q19) S
    on (S.PERSON_ID = T.PERSON_ID
   and  S.SRC_SYS_ID = T.SRC_SYS_ID)
  when matched then update set
       T.ETHNIC_GRP_FED_CD = S.ETHNIC_GRP_FED_CD,
       T.ETHNIC_GRP_ST_CD = S.ETHNIC_GRP_ST_CD,
       T.UNDER_MINORITY_FLAG = S.UNDER_MINORITY_FLAG,
       T.LASTUPD_EW_DTTM = SYSDATE
 where decode(T.ETHNIC_GRP_FED_CD,S.ETHNIC_GRP_FED_CD,0,1) = 1 or
       decode(T.ETHNIC_GRP_ST_CD,S.ETHNIC_GRP_ST_CD,0,1) = 1 or
       decode(T.UNDER_MINORITY_FLAG,S.UNDER_MINORITY_FLAG,0,1) = 1
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_PERSON rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_PERSON',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_D_PERSON';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_D_PERSON';
update CSMRT_OWNER.PS_D_PERSON T
       set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.PERSON_SID < 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PS_PERSON S
                    where T.PERSON_ID = S.EMPLID
                      and  T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_PERSON rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_PERSON',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

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

END PS_D_PERSON_P;
/

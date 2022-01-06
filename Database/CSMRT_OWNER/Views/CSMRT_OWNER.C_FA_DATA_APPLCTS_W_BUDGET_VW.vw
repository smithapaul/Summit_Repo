CREATE OR REPLACE VIEW C_FA_DATA_APPLCTS_W_BUDGET_VW
BEQUEATH DEFINER
AS 
WITH RUN_CNTRL as (
                  SELECT C_INSTITUTION, C_PERIOD, C_AID_YEAR 
                    FROM IRSTG_OWNER.CENSUS_RUN_CNTRL_TBL
                   WHERE C_INSTITUTION in ('UMBOS', 'UMDAR')
                     AND CENSUS_TO_RUN = 'Y'  
                  ),
SAWITH0 AS (select /*+ parallel(8) inline */
     sum(T1002438.ACCEPT_BALANCE) as c1,
     sum(T1002438.DISBURSED_BALANCE) as c2,
     case  when T998546.P_FATHER_GRADE_LVL_LD in ('High School', 'Middle School') 
	        and T998546.P_MOTHER_GRADE_LVL_LD in ('High School', 'Middle School') then 'First Gen' 
		   when T998546.P_FATHER_GRADE_LVL_LD in ('High School', 'Middle School') 
		    and T998546.P_MOTHER_GRADE_LVL_LD in ('Unknown') then 'First Gen' 
		   when T998546.P_FATHER_GRADE_LVL_LD in ('Unknown') 
		    and T998546.P_MOTHER_GRADE_LVL_LD in ('High School', 'Middle School') then 'First Gen' 
		   when T998546.P_MOTHER_GRADE_LVL_LD is null 
		    and T998546.P_FATHER_GRADE_LVL_LD in ('High School', 'Middle School') then 'First Gen' 
		   when T998546.P_FATHER_GRADE_LVL_LD is null 
		    and T998546.P_MOTHER_GRADE_LVL_LD in ('High School', 'Middle School') then 'First Gen' 
		   when T998546.P_FATHER_GRADE_LVL_LD in ('Unknown') 
		    and T998546.P_MOTHER_GRADE_LVL_LD in ('Unknown') then 'Unknown' 
		   when T998546.P_MOTHER_GRADE_LVL_LD not in ('College') 
		    and not T998546.P_FATHER_GRADE_LVL_LD is null then T998546.P_FATHER_GRADE_LVL_LD 
		   when not T998546.P_FATHER_GRADE_LVL_LD is null 
		    and T998546.P_MOTHER_GRADE_LVL_LD is null then T998546.P_FATHER_GRADE_LVL_LD 
		   else T998546.P_MOTHER_GRADE_LVL_LD 
	  end as c3,
     case when T998546.S_SCHOOL_CHOICE_1 = '002222' then T998546.S_HOUSING_CODE_1_LD 
	      when T998546.S_SCHOOL_CHOICE_2 = '002222' then T998546.S_HOUSING_CODE_2_LD 
		  when T998546.S_SCHOOL_CHOICE_3 = '002222' then T998546.S_HOUSING_CODE_3_LD 
		  when T998546.S_SCHOOL_CHOICE_4 = '002222' then T998546.S_HOUSING_CODE_4_LD 
		  when T998546.S_SCHOOL_CHOICE_5 = '002222' then T998546.S_HOUSING_CODE_5_LD 
		  when T998546.S_SCHOOL_CHOICE_6 = '002222' then T998546.S_HOUSING_CODE_6_LD 
		  when T998546.S_SFA_SCHL_CHOICE_7 = '002222' then T998546.S_SFA_HOUSING_CODE7_LD 
		  when T998546.S_SFA_SCHL_CHOICE_8 = '002222' then T998546.S_SFA_HOUSING_CODE8_LD 
		  when T998546.S_SFA_SCHL_CHOICE_9 = '002222' then T998546.S_SFA_HOUSING_CODE9_LD 
		  when T998546.S_SFA_SCHL_CHOICE_10 = '002222' then T998546.S_SFA_HOUSING_CODE10_LD 
	 else ' ' end as c4,
     T1008770.FED_YEAR_COA as c5,
     T1008770.FED_NEED as c6,
     T998546.AID_YEAR as c7,
     T572353.LAST_NM as c8,
     T572353.FIRST_NM as c9,
     T572353.MIDDLE_NM as c10,
     T1002439.ACAD_CAR_CD as c11,
     T998546.S_DEPNDNCY_STAT as c12,
     T998546.EFC_STATUS as c13,
     T998546.S_FISAP_TOT_INC as c14,
     T998546.PRIMARY_EFC as c15,
     T998546.PRORATED_EFC as c16,
     T572353.PERSON_ID as c17
from CSMRT_OWNER.UM_D_TERM_VW T1020466 /* FA Award Disb - D_TERM */ ,
     CSMRT_OWNER.UM_D_FA_ITEM_TYPE_VW T1006851 /* FA - D_FA_ITEM_TYPE */ ,
     CSMRT_OWNER.UM_D_ACAD_CAR_VW T1002439 /* FA Term - D_ACAD_CAR */ ,
     CSMRT_OWNER.UM_D_PERSON_CS_VW T572353 /* D_PERSON */ ,
     CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR_VW T998546 /* F_FA_STDNT_AID_ISIR */  
left outer join CSMRT_OWNER.UM_F_FA_STDNT_AWRD_PERIOD_VW T1008770 /* F_FA_STDNT_AWRD_PERIOD */  
  On T998546.AID_YEAR = T1008770.AID_YEAR 
 and T998546.INSTITUTION_CD = T1008770.INSTITUTION_CD 
 and T998546.PERSON_ID = T1008770.PERSON_ID 
 and T998546.SRC_SYS_ID = T1008770.SRC_SYS_ID,
     CSMRT_OWNER.UM_F_FA_AWARD_DISB_VW T1002438 /* F_FA_AWARD_DISB */ ,
     CSMRT_OWNER.UM_F_FA_TERM_VW T1002443 /* F_FA_TERM */ ,
     CSMRT_OWNER.UM_F_FA_STDNT_AWARDS_VW T1020465 /* F_FA_STDNT_AWARDS */ ,
     CSMRT_OWNER.UM_D_TERM_VW T1002440 /* FA Term - D_TERM */ 
where ( T1006851.FIN_AID_TYPE_LD not in ('Waiver') 
  and T1006851.ITEM_TYPE not in ('901200000800', '901200002400', '901200002500') 
  and T1002438.TERM_SID = T1020466.TERM_SID 
  and T572353.PERSON_SID = T998546.PERSON_SID 
  and T1002439.ACAD_CAR_SID = NVL(T1002443.ACAD_CAR_SID, 2147483646) 
  and T998546.AID_YEAR = T1002443.AID_YEAR 
  and T998546.INSTITUTION_CD = T1002443.INSTITUTION_CD 
  and T998546.PERSON_ID = T1002443.PERSON_ID 
  and T998546.SRC_SYS_ID = T1002443.SRC_SYS_ID 
  and T998546.AID_YEAR = T1020465.AID_YEAR 
  and T998546.INSTITUTION_CD = T1020465.INSTITUTION_CD 
  and T998546.PERSON_ID = T1020465.PERSON_ID 
  and T998546.SRC_SYS_ID = T1020465.SRC_SYS_ID 
  and T1002440.TERM_SID = NVL(T1002443.TERM_SID, 2147483646) 
  and T1002438.AID_YEAR = T1020465.AID_YEAR 
  and T1002438.INSTITUTION_CD = T1020465.INSTITUTION_CD 
  and T1002438.PERSON_ID = T1020465.PERSON_ID 
  and T1002438.ACAD_CAR_CD = T1020465.ACAD_CAR_CD 
  and T1002438.ITEM_TYPE = T1020465.ITEM_TYPE 
  and T1002438.SRC_SYS_ID = T1020465.SRC_SYS_ID 
  --and T998546.AID_YEAR = '2016'                               -- Parameter
  and T998546.AID_YEAR = (SELECT C_AID_YEAR FROM RUN_CNTRL)                             -- Parameter
  and T1002440.TERM_CD = T1020466.TERM_CD 
  --and T1002440.TERM_CD = '2510'                               -- Parameter
  and T1002440.TERM_CD = (SELECT C_PERIOD FROM RUN_CNTRL)                               -- Parameter
  --and T1002443.AID_YEAR = '2016'                              -- Parameter 
  and T1002443.AID_YEAR = (SELECT C_AID_YEAR FROM RUN_CNTRL)                              -- Parameter 
  and T1006851.ITEM_TYPE_SID = T1020465.ITEM_TYPE_SID 
  and T1008770.AWARD_PERIOD = 'A' 
  and (T998546.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL)))              -- Parameter
  and (T1002440.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL)))             -- Parameter
  and (T1002439.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL)))             -- Parameter
  and (T1006851.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL)))             -- Parameter
  and (T1002443.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL)))             -- Parameter
  and (T1020466.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL))) )           -- Parameter
group by T572353.PERSON_ID, T572353.LAST_NM, T572353.MIDDLE_NM, T572353.FIRST_NM, T998546.AID_YEAR, 
         T998546.EFC_STATUS, T998546.PRIMARY_EFC, T998546.PRORATED_EFC, T998546.S_FISAP_TOT_INC, 
		 T998546.S_DEPNDNCY_STAT, T1002439.ACAD_CAR_CD, T1008770.FED_NEED, T1008770.FED_YEAR_COA, 
 case when T998546.P_FATHER_GRADE_LVL_LD in ('High School', 'Middle School') 
       and T998546.P_MOTHER_GRADE_LVL_LD in ('High School', 'Middle School') then 'First Gen' 
	   when T998546.P_FATHER_GRADE_LVL_LD in ('High School', 'Middle School') 
	   and T998546.P_MOTHER_GRADE_LVL_LD in ('Unknown') then 'First Gen' 
	  when T998546.P_FATHER_GRADE_LVL_LD in ('Unknown') 
	  and T998546.P_MOTHER_GRADE_LVL_LD in ('High School', 'Middle School') then 'First Gen' 
	  when T998546.P_MOTHER_GRADE_LVL_LD is null 
	   and T998546.P_FATHER_GRADE_LVL_LD in ('High School', 'Middle School') then 'First Gen' 
	  when T998546.P_FATHER_GRADE_LVL_LD is null 
	   and T998546.P_MOTHER_GRADE_LVL_LD in ('High School', 'Middle School') then 'First Gen' 
	  when T998546.P_FATHER_GRADE_LVL_LD in ('Unknown') 
	   and T998546.P_MOTHER_GRADE_LVL_LD in ('Unknown') then 'Unknown' 
	  when T998546.P_MOTHER_GRADE_LVL_LD not in ('College') 
	   and not T998546.P_FATHER_GRADE_LVL_LD is null then T998546.P_FATHER_GRADE_LVL_LD 
	  when not T998546.P_FATHER_GRADE_LVL_LD is null 
	   and T998546.P_MOTHER_GRADE_LVL_LD is null then T998546.P_FATHER_GRADE_LVL_LD 
	  else T998546.P_MOTHER_GRADE_LVL_LD 
  end, 
 case when T998546.S_SCHOOL_CHOICE_1 = '002222' then T998546.S_HOUSING_CODE_1_LD 
      when T998546.S_SCHOOL_CHOICE_2 = '002222' then T998546.S_HOUSING_CODE_2_LD 
	  when T998546.S_SCHOOL_CHOICE_3 = '002222' then T998546.S_HOUSING_CODE_3_LD 
	  when T998546.S_SCHOOL_CHOICE_4 = '002222' then T998546.S_HOUSING_CODE_4_LD 
	  when T998546.S_SCHOOL_CHOICE_5 = '002222' then T998546.S_HOUSING_CODE_5_LD 
	  when T998546.S_SCHOOL_CHOICE_6 = '002222' then T998546.S_HOUSING_CODE_6_LD 
	  when T998546.S_SFA_SCHL_CHOICE_7 = '002222' then T998546.S_SFA_HOUSING_CODE7_LD 
	  when T998546.S_SFA_SCHL_CHOICE_8 = '002222' then T998546.S_SFA_HOUSING_CODE8_LD 
	  when T998546.S_SFA_SCHL_CHOICE_9 = '002222' then T998546.S_SFA_HOUSING_CODE9_LD 
	  when T998546.S_SFA_SCHL_CHOICE_10 = '002222' then T998546.S_SFA_HOUSING_CODE10_LD 
	  else ' ' 
  end),
SAWITH1 AS (
select /*+ parallel(8) inline */
       sum(T1002438.ACCEPT_BALANCE) as c18,
       count(distinct T1020466.TERM_CD) as c19,
       T572353.PERSON_ID as c20
  from CSMRT_OWNER.UM_D_TERM_VW T1020466 /* FA Award Disb - D_TERM */ ,
       CSMRT_OWNER.UM_D_FA_ITEM_TYPE_VW T1006851 /* FA - D_FA_ITEM_TYPE */ ,
       CSMRT_OWNER.UM_D_ACAD_CAR_VW T1002439 /* FA Term - D_ACAD_CAR */ ,
       CSMRT_OWNER.UM_D_PERSON_CS_VW T572353 /* D_PERSON */ ,
       CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR_VW T998546 /* F_FA_STDNT_AID_ISIR */  
  left outer join CSMRT_OWNER.UM_F_FA_STDNT_AWRD_PERIOD_VW T1008770 /* F_FA_STDNT_AWRD_PERIOD */  
    On T998546.AID_YEAR = T1008770.AID_YEAR 
   and T998546.INSTITUTION_CD = T1008770.INSTITUTION_CD 
   and T998546.PERSON_ID = T1008770.PERSON_ID 
   and T998546.SRC_SYS_ID = T1008770.SRC_SYS_ID,
       CSMRT_OWNER.UM_F_FA_AWARD_DISB_VW T1002438 /* F_FA_AWARD_DISB */ ,
       CSMRT_OWNER.UM_F_FA_TERM_VW T1002443 /* F_FA_TERM */ ,
       CSMRT_OWNER.UM_F_FA_STDNT_AWARDS_VW T1020465 /* F_FA_STDNT_AWARDS */ ,
       CSMRT_OWNER.UM_D_TERM_VW T1002440 /* FA Term - D_TERM */ 
 where ( T1006851.FIN_AID_TYPE_LD not in ('Waiver') 
    and T1006851.ITEM_TYPE not in ('901200000800', '901200002400', '901200002500') 
	and T1002438.TERM_SID = T1020466.TERM_SID 
	and T572353.PERSON_SID = T998546.PERSON_SID 
	and T1002439.ACAD_CAR_SID = NVL(T1002443.ACAD_CAR_SID, 2147483646) 
	and T998546.AID_YEAR = T1002443.AID_YEAR 
	and T998546.INSTITUTION_CD = T1002443.INSTITUTION_CD 
	and T998546.PERSON_ID = T1002443.PERSON_ID 
	and T998546.SRC_SYS_ID = T1002443.SRC_SYS_ID 
	and T998546.AID_YEAR = T1020465.AID_YEAR 
	and T998546.INSTITUTION_CD = T1020465.INSTITUTION_CD 
	and T998546.PERSON_ID = T1020465.PERSON_ID 
	and T998546.SRC_SYS_ID = T1020465.SRC_SYS_ID 
	and T1002440.TERM_SID = NVL(T1002443.TERM_SID, 2147483646) 
	and T1002438.AID_YEAR = T1020465.AID_YEAR 
	and T1002438.INSTITUTION_CD = T1020465.INSTITUTION_CD 
	and T1002438.PERSON_ID = T1020465.PERSON_ID 
	and T1002438.ACAD_CAR_CD = T1020465.ACAD_CAR_CD 
	and T1002438.ITEM_TYPE = T1020465.ITEM_TYPE 
	and T1002438.SRC_SYS_ID = T1020465.SRC_SYS_ID 
	--and T998546.AID_YEAR = '2016'                         -- Parameter
	and T998546.AID_YEAR = (SELECT C_AID_YEAR FROM RUN_CNTRL)
	and T1002440.TERM_CD = T1020466.TERM_CD 
	--and T1002440.TERM_CD = '2510'                         -- Parameter 
	and T1002440.TERM_CD = (SELECT C_PERIOD FROM RUN_CNTRL)
	--and T1002443.AID_YEAR = '2016'                        -- Parameter
	and T1002443.AID_YEAR = (SELECT C_AID_YEAR FROM RUN_CNTRL)
	and T1006851.ITEM_TYPE_SID = T1020465.ITEM_TYPE_SID 
	and T1008770.AWARD_PERIOD = 'A' 
	and (T998546.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL)))        -- Parameter
	and (T1002440.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL)))       -- Parameter
	and (T1002439.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL)))       -- Parameter
	and (T1006851.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL)))       -- Parameter
	and (T1002443.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL)))       -- Parameter
	and (T1020466.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL))) )     -- Parameter
group by T572353.PERSON_ID),
SAWITH2 AS (select D1.c1 as c1,
     D1.c2 as c2,
     D1.c3 as c3,
     D1.c4 as c4,
     D1.c5 as c5,
     D1.c6 as c6,
     D1.c7 as c7,
     D1.c8 as c8,
     D1.c9 as c9,
     D1.c10 as c10,
     D1.c11 as c11,
     D1.c12 as c12,
     D1.c13 as c13,
     D1.c14 as c14,
     D1.c15 as c15,
     D1.c16 as c16,
     D2.c18 as c17,
     D2.c19 as c18,
     D1.c17 as c19
from SAWITH0 D1 
inner join SAWITH1 D2 
  On  SYS_OP_MAP_NONNULL(D2.c20) = SYS_OP_MAP_NONNULL(D1.c17) 
where ( 0 < D1.c1 ) ),
SAWITH3 AS (select /*+ parallel(8) inline */
     distinct T1064479.TERM_CD as c1,
     T572353.PERSON_ID as c2,
     T572353.FIRST_NM as c3,
     T572353.MIDDLE_NM as c4,
     T572353.LAST_NM as c5,
     T1002439.ACAD_CAR_CD as c6,
     T998546.PRORATED_EFC as c7,
     T998546.EFC_STATUS as c8,
     T998546.PRIMARY_EFC as c9,
     T998546.S_DEPNDNCY_STAT as c10,
     T998546.S_FISAP_TOT_INC as c11,
     T998546.AID_YEAR as c12,
     T1008770.FED_YEAR_COA as c13,
     T1008770.FED_NEED as c14,
     case  when T998546.P_FATHER_GRADE_LVL_LD in ('High School', 'Middle School') 
	        and T998546.P_MOTHER_GRADE_LVL_LD in ('High School', 'Middle School') then 'First Gen' 
		   when T998546.P_FATHER_GRADE_LVL_LD in ('High School', 'Middle School') 
		    and T998546.P_MOTHER_GRADE_LVL_LD in ('Unknown') then 'First Gen' 
		   when T998546.P_FATHER_GRADE_LVL_LD in ('Unknown') 
		    and T998546.P_MOTHER_GRADE_LVL_LD in ('High School', 'Middle School') then 'First Gen' 
		   when T998546.P_MOTHER_GRADE_LVL_LD is null 
		    and T998546.P_FATHER_GRADE_LVL_LD in ('High School', 'Middle School') then 'First Gen' 
		   when T998546.P_FATHER_GRADE_LVL_LD is null 
		    and T998546.P_MOTHER_GRADE_LVL_LD in ('High School', 'Middle School') then 'First Gen' 
		   when T998546.P_FATHER_GRADE_LVL_LD in ('Unknown') 
		    and T998546.P_MOTHER_GRADE_LVL_LD in ('Unknown') then 'Unknown' 
		   when T998546.P_MOTHER_GRADE_LVL_LD not in ('College') 
		    and not T998546.P_FATHER_GRADE_LVL_LD is null then T998546.P_FATHER_GRADE_LVL_LD 
		   when not T998546.P_FATHER_GRADE_LVL_LD is null 
		    and T998546.P_MOTHER_GRADE_LVL_LD is null then T998546.P_FATHER_GRADE_LVL_LD 
		   else T998546.P_MOTHER_GRADE_LVL_LD 
	  end  as c15,
     case  when T998546.S_SCHOOL_CHOICE_1 = '002222' then T998546.S_HOUSING_CODE_1_LD 
	       when T998546.S_SCHOOL_CHOICE_2 = '002222' then T998546.S_HOUSING_CODE_2_LD 
		   when T998546.S_SCHOOL_CHOICE_3 = '002222' then T998546.S_HOUSING_CODE_3_LD 
		   when T998546.S_SCHOOL_CHOICE_4 = '002222' then T998546.S_HOUSING_CODE_4_LD 
		   when T998546.S_SCHOOL_CHOICE_5 = '002222' then T998546.S_HOUSING_CODE_5_LD 
		   when T998546.S_SCHOOL_CHOICE_6 = '002222' then T998546.S_HOUSING_CODE_6_LD 
		   when T998546.S_SFA_SCHL_CHOICE_7 = '002222' then T998546.S_SFA_HOUSING_CODE7_LD 
		   when T998546.S_SFA_SCHL_CHOICE_8 = '002222' then T998546.S_SFA_HOUSING_CODE8_LD 
		   when T998546.S_SFA_SCHL_CHOICE_9 = '002222' then T998546.S_SFA_HOUSING_CODE9_LD 
		   when T998546.S_SFA_SCHL_CHOICE_10 = '002222' then T998546.S_SFA_HOUSING_CODE10_LD 
	       else ' ' 
	  end  as c16
from 
     CSMRT_OWNER.UM_D_TERM_VW T1064479 /* FA Budget - D_TERM */ ,
     CSMRT_OWNER.UM_D_ACAD_CAR_VW T1002439 /* FA Term - D_ACAD_CAR */ ,
     CSMRT_OWNER.UM_D_PERSON_CS_VW T572353 /* D_PERSON */ ,
     CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR_VW T998546 /* F_FA_STDNT_AID_ISIR */  
left outer join CSMRT_OWNER.UM_F_FA_STDNT_AWRD_PERIOD_VW T1008770 /* F_FA_STDNT_AWRD_PERIOD */  
             On T998546.AID_YEAR = T1008770.AID_YEAR 
			and T998546.INSTITUTION_CD = T1008770.INSTITUTION_CD 
			and T998546.PERSON_ID = T1008770.PERSON_ID 
			and T998546.SRC_SYS_ID = T1008770.SRC_SYS_ID,
     CSMRT_OWNER.UM_F_FA_TERM_VW T1002443 /* F_FA_TERM */ ,
     CSMRT_OWNER.UM_F_FA_STDNT_BDGT_ITEM_VW T1017393 /* F_FA_STDNT_BDGT_ITEM */ ,
     CSMRT_OWNER.UM_F_FA_STDNT_BDGT_VW T1017398 /* F_FA_STDNT_BDGT */ ,
     CSMRT_OWNER.UM_D_TERM_VW T1002440 /* FA Term - D_TERM */ 
where ( T572353.PERSON_SID = T998546.PERSON_SID 
  and T1002439.ACAD_CAR_SID = NVL(T1002443.ACAD_CAR_SID, 2147483646) 
  and T998546.AID_YEAR = T1002443.AID_YEAR 
  and T998546.INSTITUTION_CD = T1002443.INSTITUTION_CD 
  and T998546.PERSON_ID = T1002443.PERSON_ID 
  and T998546.SRC_SYS_ID = T1002443.SRC_SYS_ID 
  and T998546.AID_YEAR = T1017398.AID_YEAR 
  and T998546.INSTITUTION_CD = T1017398.INSTITUTION_CD 
  and T998546.PERSON_ID = T1017398.PERSON_ID 
  and T998546.SRC_SYS_ID = T1017398.SRC_SYS_ID 
  and T1002440.TERM_SID = NVL(T1002443.TERM_SID, 2147483646) 
  and T1017393.ACAD_CAR_CD = T1017398.ACAD_CAR_CD 
  and T1017393.AID_YEAR = T1017398.AID_YEAR 
  and T1017393.INSTITUTION_CD = T1017398.INSTITUTION_CD 
  and T1017393.PERSON_ID = T1017398.PERSON_ID 
  and T1017393.SRC_SYS_ID = T1017398.SRC_SYS_ID 
  and T1017393.TERM_CD = T1017398.TERM_CD 
  --and T998546.AID_YEAR = '2016'                 -- Parameter
  and T998546.AID_YEAR = (SELECT C_AID_YEAR FROM RUN_CNTRL)
  and T1002440.TERM_CD = T1064479.TERM_CD 
  --and T1002440.TERM_CD = '2510'                 -- Parameter
  and T1002440.TERM_CD = (SELECT C_PERIOD FROM RUN_CNTRL)
  --and T1002443.AID_YEAR = '2016'                -- Parameter
  and T1002443.AID_YEAR = (SELECT C_AID_YEAR FROM RUN_CNTRL)
  and T1008770.AWARD_PERIOD = 'A' 
  and T1017398.TERM_SID = T1064479.TERM_SID 
  and (T998546.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL)))  -- Parameter 
  and (T1002439.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL))) -- Parameter 
  and (T1002440.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL))) -- Parameter
  and (T1002443.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL))) -- Parameter
  and (T1064479.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL))) -- Parameter
  and (T1017393.BUDGET_ITEM_CD in ('CE F', 'CE T', 'ST F', 'ST T')) ) ),
SAWITH4 AS (select /*+ parallel(8) inline */
           sum(T1017393.BUDGET_ITEM_AMOUNT) as c1,
     case when T998546.P_FATHER_GRADE_LVL_LD in ('High School', 'Middle School') 
	       and T998546.P_MOTHER_GRADE_LVL_LD in ('High School', 'Middle School') then 'First Gen' 
		  when T998546.P_FATHER_GRADE_LVL_LD in ('High School', 'Middle School') 
		   and T998546.P_MOTHER_GRADE_LVL_LD in ('Unknown') then 'First Gen' 
		  when T998546.P_FATHER_GRADE_LVL_LD in ('Unknown') 
		   and T998546.P_MOTHER_GRADE_LVL_LD in ('High School', 'Middle School') then 'First Gen' 
		  when T998546.P_MOTHER_GRADE_LVL_LD is null 
		   and T998546.P_FATHER_GRADE_LVL_LD in ('High School', 'Middle School') then 'First Gen' 
		  when T998546.P_FATHER_GRADE_LVL_LD is null 
		   and T998546.P_MOTHER_GRADE_LVL_LD in ('High School', 'Middle School') then 'First Gen' 
		  when T998546.P_FATHER_GRADE_LVL_LD in ('Unknown') 
		   and T998546.P_MOTHER_GRADE_LVL_LD in ('Unknown') then 'Unknown' 
		  when T998546.P_MOTHER_GRADE_LVL_LD not in ('College') 
		   and not T998546.P_FATHER_GRADE_LVL_LD is null then T998546.P_FATHER_GRADE_LVL_LD 
		  when not T998546.P_FATHER_GRADE_LVL_LD is null 
		   and T998546.P_MOTHER_GRADE_LVL_LD is null then T998546.P_FATHER_GRADE_LVL_LD 
		  else T998546.P_MOTHER_GRADE_LVL_LD 
	  end as c2,
     case when T998546.S_SCHOOL_CHOICE_1 = '002222' then T998546.S_HOUSING_CODE_1_LD 
	      when T998546.S_SCHOOL_CHOICE_2 = '002222' then T998546.S_HOUSING_CODE_2_LD 
		  when T998546.S_SCHOOL_CHOICE_3 = '002222' then T998546.S_HOUSING_CODE_3_LD 
		  when T998546.S_SCHOOL_CHOICE_4 = '002222' then T998546.S_HOUSING_CODE_4_LD 
		  when T998546.S_SCHOOL_CHOICE_5 = '002222' then T998546.S_HOUSING_CODE_5_LD 
		  when T998546.S_SCHOOL_CHOICE_6 = '002222' then T998546.S_HOUSING_CODE_6_LD 
		  when T998546.S_SFA_SCHL_CHOICE_7 = '002222' then T998546.S_SFA_HOUSING_CODE7_LD 
		  when T998546.S_SFA_SCHL_CHOICE_8 = '002222' then T998546.S_SFA_HOUSING_CODE8_LD 
		  when T998546.S_SFA_SCHL_CHOICE_9 = '002222' then T998546.S_SFA_HOUSING_CODE9_LD 
		  when T998546.S_SFA_SCHL_CHOICE_10 = '002222' then T998546.S_SFA_HOUSING_CODE10_LD 
		  else ' ' 
	  end as c3,
     T1008770.FED_YEAR_COA as c4,
     T1008770.FED_NEED as c5,
     T998546.AID_YEAR as c6,
     T572353.PERSON_ID as c7,
     T572353.LAST_NM as c8,
     T572353.FIRST_NM as c9,
     T572353.MIDDLE_NM as c10,
     T1002439.ACAD_CAR_CD as c11,
     T998546.S_DEPNDNCY_STAT as c12,
     T998546.EFC_STATUS as c13,
     T998546.S_FISAP_TOT_INC as c14,
     T998546.PRIMARY_EFC as c15,
     T998546.PRORATED_EFC as c16
from 
     CSMRT_OWNER.UM_D_TERM_VW T1064479 /* FA Budget - D_TERM */ ,
     CSMRT_OWNER.UM_D_ACAD_CAR_VW T1002439 /* FA Term - D_ACAD_CAR */ ,
     CSMRT_OWNER.UM_D_PERSON_CS_VW T572353 /* D_PERSON */ ,
     CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR_VW T998546 /* F_FA_STDNT_AID_ISIR */  
left outer join CSMRT_OWNER.UM_F_FA_STDNT_AWRD_PERIOD_VW T1008770 /* F_FA_STDNT_AWRD_PERIOD */  
             On T998546.AID_YEAR = T1008770.AID_YEAR 
			and T998546.INSTITUTION_CD = T1008770.INSTITUTION_CD 
			and T998546.PERSON_ID = T1008770.PERSON_ID 
			and T998546.SRC_SYS_ID = T1008770.SRC_SYS_ID,
     CSMRT_OWNER.UM_F_FA_TERM_VW T1002443 /* F_FA_TERM */ ,
     CSMRT_OWNER.UM_F_FA_STDNT_BDGT_ITEM_VW T1017393 /* F_FA_STDNT_BDGT_ITEM */ ,
     CSMRT_OWNER.UM_F_FA_STDNT_BDGT_VW T1017398 /* F_FA_STDNT_BDGT */ ,
     CSMRT_OWNER.UM_D_TERM_VW T1002440 /* FA Term - D_TERM */ 
where ( T572353.PERSON_SID = T998546.PERSON_SID 
  and T1002439.ACAD_CAR_SID = NVL(T1002443.ACAD_CAR_SID, 2147483646) 
  and T998546.AID_YEAR = T1002443.AID_YEAR 
  and T998546.INSTITUTION_CD = T1002443.INSTITUTION_CD 
  and T998546.PERSON_ID = T1002443.PERSON_ID 
  and T998546.SRC_SYS_ID = T1002443.SRC_SYS_ID 
  and T998546.AID_YEAR = T1017398.AID_YEAR 
  and T998546.INSTITUTION_CD = T1017398.INSTITUTION_CD 
  and T998546.PERSON_ID = T1017398.PERSON_ID 
  and T998546.SRC_SYS_ID = T1017398.SRC_SYS_ID 
  and T1002440.TERM_SID = NVL(T1002443.TERM_SID, 2147483646) 
  and T1017393.ACAD_CAR_CD = T1017398.ACAD_CAR_CD 
  and T1017393.AID_YEAR = T1017398.AID_YEAR 
  and T1017393.INSTITUTION_CD = T1017398.INSTITUTION_CD 
  and T1017393.PERSON_ID = T1017398.PERSON_ID 
  and T1017393.SRC_SYS_ID = T1017398.SRC_SYS_ID 
  and T1017393.TERM_CD = T1017398.TERM_CD 
  --and T998546.AID_YEAR = '2016'                          -- Parameter
  and T998546.AID_YEAR = (SELECT C_AID_YEAR from RUN_CNTRL)  -- Parameter
  and T1002440.TERM_CD = T1064479.TERM_CD 
  --and T1002440.TERM_CD = '2510'                          -- Parameter
  and T1002440.TERM_CD = (SELECT C_PERIOD from RUN_CNTRL)                          -- Parameter
  --and T1002443.AID_YEAR = '2016'                         -- Parameter
  and T1002443.AID_YEAR = (SELECT C_AID_YEAR from RUN_CNTRL)                       -- Parameter
  and T1008770.AWARD_PERIOD = 'A' 
  and T1017398.TERM_SID = T1064479.TERM_SID 
  and (T998546.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL)))         -- Parameter
  and (T1002439.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL)))        -- Parameter
  and (T1002440.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL)))        -- Parameter
  and (T1002443.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL)))        -- Parameter
  and (T1064479.INSTITUTION_CD in ('-', (SELECT C_INSTITUTION FROM RUN_CNTRL)))        -- Parameter
  and (T1017393.BUDGET_ITEM_CD in ('CE F', 'CE T', 'ST F', 'ST T')) ) 
group by T572353.PERSON_ID, T572353.LAST_NM, T572353.MIDDLE_NM, T572353.FIRST_NM, T998546.AID_YEAR, 
         T998546.EFC_STATUS, T998546.PRIMARY_EFC, T998546.PRORATED_EFC, T998546.S_FISAP_TOT_INC, 
		 T998546.S_DEPNDNCY_STAT, T1002439.ACAD_CAR_CD, T1008770.FED_NEED, T1008770.FED_YEAR_COA, 
 case when T998546.P_FATHER_GRADE_LVL_LD in ('High School', 'Middle School') 
       and T998546.P_MOTHER_GRADE_LVL_LD in ('High School', 'Middle School') then 'First Gen' 
	  when T998546.P_FATHER_GRADE_LVL_LD in ('High School', 'Middle School') 
	   and T998546.P_MOTHER_GRADE_LVL_LD in ('Unknown') then 'First Gen' 
	  when T998546.P_FATHER_GRADE_LVL_LD in ('Unknown') 
	   and T998546.P_MOTHER_GRADE_LVL_LD in ('High School', 'Middle School') then 'First Gen' 
	  when T998546.P_MOTHER_GRADE_LVL_LD is null 
	   and T998546.P_FATHER_GRADE_LVL_LD in ('High School', 'Middle School') then 'First Gen' 
	  when T998546.P_FATHER_GRADE_LVL_LD is null 
	   and T998546.P_MOTHER_GRADE_LVL_LD in ('High School', 'Middle School') then 'First Gen' 
	  when T998546.P_FATHER_GRADE_LVL_LD in ('Unknown') 
	   and T998546.P_MOTHER_GRADE_LVL_LD in ('Unknown') then 'Unknown' 
	  when T998546.P_MOTHER_GRADE_LVL_LD not in ('College') 
	   and not T998546.P_FATHER_GRADE_LVL_LD is null then T998546.P_FATHER_GRADE_LVL_LD 
	  when not T998546.P_FATHER_GRADE_LVL_LD is null 
	   and T998546.P_MOTHER_GRADE_LVL_LD is null then T998546.P_FATHER_GRADE_LVL_LD 
	  else T998546.P_MOTHER_GRADE_LVL_LD 
 end, 
 case when T998546.S_SCHOOL_CHOICE_1 = '002222' then T998546.S_HOUSING_CODE_1_LD 
      when T998546.S_SCHOOL_CHOICE_2 = '002222' then T998546.S_HOUSING_CODE_2_LD 
	  when T998546.S_SCHOOL_CHOICE_3 = '002222' then T998546.S_HOUSING_CODE_3_LD 
	  when T998546.S_SCHOOL_CHOICE_4 = '002222' then T998546.S_HOUSING_CODE_4_LD 
	  when T998546.S_SCHOOL_CHOICE_5 = '002222' then T998546.S_HOUSING_CODE_5_LD 
	  when T998546.S_SCHOOL_CHOICE_6 = '002222' then T998546.S_HOUSING_CODE_6_LD 
	  when T998546.S_SFA_SCHL_CHOICE_7 = '002222' then T998546.S_SFA_HOUSING_CODE7_LD 
	  when T998546.S_SFA_SCHL_CHOICE_8 = '002222' then T998546.S_SFA_HOUSING_CODE8_LD 
	  when T998546.S_SFA_SCHL_CHOICE_9 = '002222' then T998546.S_SFA_HOUSING_CODE9_LD 
	  when T998546.S_SFA_SCHL_CHOICE_10 = '002222' then T998546.S_SFA_HOUSING_CODE10_LD 
 else ' ' 
  end ),
SAWITH5 AS (select /*+ parallel(8) inline */
     coalesce( D1.c12, D2.c6) as c1,
     coalesce( D1.c2, D2.c7) as c2,
     coalesce( D1.c5, D2.c8) as c3,
     coalesce( D1.c3, D2.c9) as c4,
     coalesce( D1.c4, D2.c10) as c5,
     coalesce( D1.c6, D2.c11) as c6,
     ' ' as c7,
     coalesce( D1.c10, D2.c12) as c8,
     coalesce( D1.c8, D2.c13) as c9,
     coalesce( D1.c14, D2.c5) as c10,
     coalesce( D1.c11, D2.c14) as c11,
     coalesce( D1.c9, D2.c15) as c12,
     coalesce( D1.c7, D2.c16) as c13,
     coalesce( D1.c13, D2.c4) as c14,
     coalesce( D1.c16, D2.c3) as c15,
     coalesce( D1.c15, D2.c2) as c16,
     0 as c17,
     0 as c18,
     0 as c19,
     D2.c1 as c21,
     D1.c1 as c22,
     ROW_NUMBER() OVER (PARTITION BY coalesce( D1.c2, D2.c7), D1.c1, coalesce( D1.c3, D2.c9), coalesce( D1.c4, D2.c10), 
	                                 coalesce( D1.c5, D2.c8), coalesce( D1.c12, D2.c6), coalesce( D1.c6, D2.c11), 
									 coalesce( D1.c13, D2.c4), coalesce( D1.c14, D2.c5), coalesce( D1.c7, D2.c16), coalesce( D1.c8, D2.c13), 
									 coalesce( D1.c9, D2.c15), coalesce( D1.c10, D2.c12), coalesce( D1.c11, D2.c14), coalesce( D1.c15, D2.c2), 
									 coalesce( D1.c16, D2.c3) ORDER BY coalesce( D1.c2, D2.c7) DESC, D1.c1 DESC, coalesce( D1.c3, D2.c9) DESC, 
									 coalesce( D1.c4, D2.c10) DESC, coalesce( D1.c5, D2.c8) DESC, coalesce( D1.c12, D2.c6) DESC, 
									 coalesce( D1.c6, D2.c11) DESC, coalesce( D1.c13, D2.c4) DESC, coalesce( D1.c14, D2.c5) DESC, 
									 coalesce( D1.c7, D2.c16) DESC, coalesce( D1.c8, D2.c13) DESC, coalesce( D1.c9, D2.c15) DESC, 
									 coalesce( D1.c10, D2.c12) DESC, coalesce( D1.c11, D2.c14) DESC, coalesce( D1.c15, D2.c2) DESC, 
									 coalesce( D1.c16, D2.c3) DESC) as c23
from SAWITH3 D1 
full outer join SAWITH4 D2 
  On SYS_OP_MAP_NONNULL(D1.c16) = SYS_OP_MAP_NONNULL(D2.c3)  
 and SYS_OP_MAP_NONNULL(D1.c15) = SYS_OP_MAP_NONNULL(D2.c2)  
 and SYS_OP_MAP_NONNULL(D1.c14) = SYS_OP_MAP_NONNULL(D2.c5)  
 and SYS_OP_MAP_NONNULL(D1.c13) = SYS_OP_MAP_NONNULL(D2.c4)  
 and D1.c12 = D2.c6 
 and SYS_OP_MAP_NONNULL(D1.c11) = SYS_OP_MAP_NONNULL(D2.c14)  
 and D1.c10 = D2.c12 
 and SYS_OP_MAP_NONNULL(D1.c9) = SYS_OP_MAP_NONNULL(D2.c15)  
 and SYS_OP_MAP_NONNULL(D1.c8) = SYS_OP_MAP_NONNULL(D2.c13)  
 and SYS_OP_MAP_NONNULL(D1.c7) = SYS_OP_MAP_NONNULL(D2.c16)  
 and D1.c6 = D2.c11 
 and SYS_OP_MAP_NONNULL(D1.c5) = SYS_OP_MAP_NONNULL(D2.c8)  
 and SYS_OP_MAP_NONNULL(D1.c4) = SYS_OP_MAP_NONNULL(D2.c10)  
 and SYS_OP_MAP_NONNULL(D1.c2) = SYS_OP_MAP_NONNULL(D2.c7)  
 and SYS_OP_MAP_NONNULL(D1.c3) = SYS_OP_MAP_NONNULL(D2.c9) 
where  ( 0 < D2.c1 ) ),
SAWITH6 AS ((select /*+ parallel(8) inline */
     D1.c1 as c1,
     D1.c2 as c2,
     D1.c3 as c3,
     D1.c4 as c4,
     D1.c5 as c5,
     D1.c6 as c6,
     D1.c7 as c7,
     D1.c8 as c8,
     D1.c9 as c9,
     D1.c10 as c10,
     D1.c11 as c11,
     D1.c12 as c12,
     D1.c13 as c13,
     D1.c14 as c14,
     D1.c15 as c15,
     D1.c16 as c16,
     D1.c17 as c17,
     D1.c18 as c18,
     D1.c19 as c19,
     D1.c20 as c20
from 
     (select D1.c7 as c1,
               D1.c19 as c2,
               D1.c8 as c3,
               D1.c9 as c4,
               D1.c10 as c5,
               D1.c11 as c6,
               ' ' as c7,
               D1.c12 as c8,
               D1.c13 as c9,
               D1.c6 as c10,
               D1.c14 as c11,
               D1.c15 as c12,
               D1.c16 as c13,
               D1.c5 as c14,
               D1.c4 as c15,
               D1.c3 as c16,
               D1.c1 as c17,
               D1.c2 as c18,
               D1.c18 as c19,
               0 as c20,
               ROW_NUMBER() OVER (PARTITION BY D1.c3, D1.c4, D1.c5, D1.c6, D1.c7, D1.c8, D1.c9, D1.c10, D1.c11, 
			                                   D1.c12, D1.c13, D1.c14, D1.c15, D1.c16, D1.c19 
								      ORDER BY D1.c3 ASC, D1.c4 ASC, D1.c5 ASC, D1.c6 ASC, D1.c7 ASC, D1.c8 ASC, 
									           D1.c9 ASC, D1.c10 ASC, D1.c11 ASC, D1.c12 ASC, D1.c13 ASC, D1.c14 ASC, 
											   D1.c15 ASC, D1.c16 ASC, D1.c19 ASC) as c21
          from SAWITH2 D1
     ) D1
where ( D1.c21 = 1 ) 
union
select D1.c1 as c1,
     D1.c2 as c2,
     D1.c3 as c3,
     D1.c4 as c4,
     D1.c5 as c5,
     D1.c6 as c6,
     D1.c7 as c7,
     D1.c8 as c8,
     D1.c9 as c9,
     D1.c10 as c10,
     D1.c11 as c11,
     D1.c12 as c12,
     D1.c13 as c13,
     D1.c14 as c14,
     D1.c15 as c15,
     D1.c16 as c16,
     D1.c17 as c17,
     D1.c18 as c18,
     D1.c19 as c19,
     D1.c20 as c20
from 
     (select D1.c1 as c1,
               D1.c2 as c2,
               D1.c3 as c3,
               D1.c4 as c4,
               D1.c5 as c5,
               D1.c6 as c6,
               D1.c7 as c7,
               D1.c8 as c8,
               D1.c9 as c9,
               D1.c10 as c10,
               D1.c11 as c11,
               D1.c12 as c12,
               D1.c13 as c13,
               D1.c14 as c14,
               D1.c15 as c15,
               D1.c16 as c16,
               D1.c17 as c17,
               D1.c18 as c18,
               D1.c19 as c19,
               sum(case D1.c23 when 1 then D1.c21 else NULL end ) over (partition by D1.c2, D1.c22)  as c20,
               ROW_NUMBER() OVER (PARTITION BY D1.c1, D1.c2, D1.c3, D1.c4, D1.c5, D1.c6, D1.c8, D1.c9, D1.c10, 
			                                   D1.c11, D1.c12, D1.c13, D1.c14, D1.c15, D1.c16 
			                          ORDER BY D1.c1 ASC, D1.c2 ASC, D1.c3 ASC, D1.c4 ASC, D1.c5 ASC, D1.c6 ASC, 
									           D1.c8 ASC, D1.c9 ASC, D1.c10 ASC, D1.c11 ASC, D1.c12 ASC, 
											   D1.c13 ASC, D1.c14 ASC, D1.c15 ASC, D1.c16 ASC) as c21
          from 
               SAWITH5 D1
     ) D1
where  ( D1.c21 = 1 ) ))
select distinct 
     D1.c1 as AID_YEAR,
     CAST(D1.c2 AS VARCHAR2(11)) as PERSON_ID,
     D1.c3 as LAST_NAME,
     D1.c4 as FIRST_NAME,
     D1.c5 as MIDDLE_NAME,
     D1.c6 as ACAD_CAR_CD,
--     D1.c7 as c7,                        
     D1.c8 as FED_DEPEND_STAT,                             -- CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR_VW
     D1.c9 as EFC_STATUS,
     D1.c10 as FEDERAL_NEED,                    --look up column name
     D1.c11 as FISAP_TOTAL_INCOME,              --look up column name
     D1.c12 as PRIMARY_EFC,                     --look up column name
     D1.c13 as PRORATED_EFC,                    --look up column name
     D1.c14 as FEDERAL_YEAR_COA,                --look up column name
     D1.c15 as HOUSING,
     D1.c16 as FIRST_GEN,
--     D1.c17 as c17,
--     D1.c18 as c18,
--     D1.c19 as c19,
--     D1.c20 as c20,
--     ROUND(max(D1.c17) over (partition by D1.c2), 0)  as MAX_ACCEPTED_BALANCE,
     --ROUND(max(D1.c18) over (partition by D1.c2), 0)  as MAX_DISBURSED_BALANCE,
     max(D1.c17) over (partition by D1.c2)  as MAX_ACCEPTED_BALANCE,
     max(D1.c18) over (partition by D1.c2)  as MAX_DISBURSED_BALANCE,	 
     max(D1.c19) over (partition by D1.c2)  as MAX_AWARD_TERM_COUNTER,
     max(D1.c20) over (partition by D1.c2)  as MAX_BUDGET_ITEM_AMT
from 
     SAWITH6 D1
--WHERE c2 = '00008164'
;

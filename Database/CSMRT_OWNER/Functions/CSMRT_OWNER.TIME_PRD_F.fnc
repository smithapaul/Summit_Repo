DROP FUNCTION CSMRT_OWNER.TIME_PRD_F
/

--
-- TIME_PRD_F  (Function) 
--
CREATE OR REPLACE function CSMRT_OWNER.time_prd_f(p_TIME_PERIOD varchar2) return varchar2
is
 v_dummy varchar2(50);
begin
 select decode(upper(p_TIME_PERIOD),
               'ALL',   'Total',
               'Q1',    'Quarter 1',
               'Q2',    'Quarter 2',
               'Q3',    'Quarter 3',
               'Q4',    'Quarter 4',
               'Error') into v_dummy from DUAL;
 return v_dummy;
exception
 when others then return 'Error';
end time_prd_f;
 
/

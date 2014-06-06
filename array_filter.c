/*-------------------------------------------------------------------------
 *
 * array_filter.c
 *	Functions for the Predictive Search project
 *
 * Authors: Cédric Villemain, Marko Tiikkaja
 *
 * Copyright (c) 2010-2011, 2ndQuadrant
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *    This product includes software developed by 2ndQuadrant.
 * 4. Neither the name of 2ndQuadrant nor the
 *    names of its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY 2NDQUADRANT ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL 2NDQUADRANT BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h" /* SRF */
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#if PG_VERSION_NUM >= 90100
#include "catalog/pg_collation.h"
#endif
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

Datum array_filter(PG_FUNCTION_ARGS);
static void
initialize_callback_fcinfo(FunctionCallInfoData *fcinfo, Oid oid,
						   ArrayType *args, Oid inputtype);

static char *
get_type_name(Oid oid);
static bool
array_get_isnull(const bits8 *nullbitmap, int offset);

/*
 * FILTER
 */
PG_FUNCTION_INFO_V1(array_filter);
Datum
array_filter(PG_FUNCTION_ARGS)
{
	typedef struct
	{
		ArrayType	   *arr;
		int				limit; /* this is the LIMIT for output */
		int				count; /* number of elements so far */
		int				nextelem;
		int				numelems;
		char		   *elemdataptr;	/* this moves with nextelem */
		bits8		   *arraynullsptr;	  /* this does not */
		int16			elmlen;
		bool			elmbyval;
		char			elmalign;
		FunctionCallInfoData callback_fcinfo;
	} array_filter_fctx;

	FuncCallContext *funcctx;
	array_filter_fctx *fctx;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		int		l;
		ArrayType  *arr;
		Oid			callback_oid;
		ArrayType  *callback_args;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/*
		 * Get the array value and detoast if needed.  We can't do this
		 * earlier because if we have to detoast, we want the detoasted copy
		 * to be in multi_call_memory_ctx, so it will go away when we're done
		 * and not before.  (If no detoast happens, we assume the originally
		 * passed array will stick around till then.)
		 */
		arr = PG_GETARG_ARRAYTYPE_P(0);
		callback_oid = PG_GETARG_OID(1); /* we can use regproc as oid */
		l   = PG_GETARG_INT32(2);
		callback_args = PG_GETARG_ARRAYTYPE_P(3);

		/* allocate memory for user context */
		fctx = (array_filter_fctx *) palloc(sizeof(array_filter_fctx));

		/* initialize state */
		initialize_callback_fcinfo(&fctx->callback_fcinfo, callback_oid,
								   callback_args, ARR_ELEMTYPE(arr));
		fctx->count	= 0;
		fctx->limit	= l;
		fctx->arr = arr;
		fctx->nextelem = 0;
		fctx->numelems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));

		fctx->elemdataptr = ARR_DATA_PTR(arr);
		fctx->arraynullsptr = ARR_NULLBITMAP(arr);

		get_typlenbyvalalign(ARR_ELEMTYPE(arr),
							 &fctx->elmlen,
							 &fctx->elmbyval,
							 &fctx->elmalign);

		funcctx->user_fctx = fctx;
		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	fctx = funcctx->user_fctx;

	while (fctx->count < fctx->limit && fctx->nextelem < fctx->numelems)
	{
		int		offset = fctx->nextelem++;
		Datum	elem;
		Datum	result;
		char   *ptr;
		bool	is_null;

		/*
		 * First see if the array element is NULL.  We can immediately omit
		 * NULL elements from our results if the callback function is strict.
		 * Otherwise we need to call the function with NULL input and let it
		 * decide.
		 */
		is_null = array_get_isnull(fctx->arraynullsptr, offset);
		if (is_null)
		{
			/* note: we don't need to move elemdataptr on NULL elements */

			if (fctx->callback_fcinfo.flinfo->fn_strict)
				continue;

			/* call with NULL input */
			elem = (Datum) 0;
		}
		else
		{
			/* not NULL. let's look at the data */
			ptr = fctx->elemdataptr;

			/* get the curent element as Datum */
			elem = fetch_att(ptr, fctx->elmbyval, fctx->elmlen);

			/* advance elemdataptr over the element */
			ptr = att_addlength_pointer(ptr, fctx->elmlen, ptr);
			ptr = (char *) att_align_nominal(ptr, fctx->elmalign);
			fctx->elemdataptr = ptr;
		}

		/* elem and is_null are initialized, now call the function */
		fctx->callback_fcinfo.arg[0] = elem;
		fctx->callback_fcinfo.argnull[0] = is_null;

		result = FunctionCallInvoke(&fctx->callback_fcinfo);

		/*
		 * If the function call evaluates to NULL, we omit the array element
		 * from out result set, similarly to a WHERE clause.
		 */
		if (!fctx->callback_fcinfo.isnull &&
			DatumGetBool(result))
		{
			fctx->count++;
			fcinfo->isnull = is_null;
			SRF_RETURN_NEXT(funcctx, elem);
		}
	}

	/* done when there is no more left */
	SRF_RETURN_DONE(funcctx);

}

/*
 * Initialize fcinfo for repeatedly calling the callback function.
 *
 * We also run a number of sanity checks on the function since
 * we're holding the syscache tuple anyway.
 *
 * Only call within multi_call_memory_ctx.
 */
static void
initialize_callback_fcinfo(FunctionCallInfoData *fcinfo, Oid oid,
						   ArrayType *callback_args, Oid inputtype)
{
	FmgrInfo	   *flinfo;
	Datum			datum;
	Oid			   *argtypes;
	int				nargs;
	HeapTuple		procTuple;
	Form_pg_proc	procStruct;
	bool			isNull;
	int				i;

	Datum		   *variadicargs;
	bool		   *variadicnulls;
	int				n_variadic;
	int16			typlen;
	bool			typbyval;
	char			typalign;
	AclResult		aclresult;


	procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(oid));
	if (!HeapTupleIsValid(procTuple))
		elog(ERROR, "cache lookup failed for function %u", oid);

	procStruct = (Form_pg_proc) GETSTRUCT(procTuple);

	/*
	 * Before going any further, check that the supplied callback function can
	 * be safely used for our job.
	 */

	/* We need to manually check that the user can call the function */
	datum = SysCacheGetAttr(PROCOID, procTuple,
							Anum_pg_proc_pronamespace,
							&isNull);
	Assert(!isNull);
	aclresult = pg_namespace_aclcheck(DatumGetObjectId(datum), GetUserId(),
									  ACL_USAGE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
					   get_namespace_name(DatumGetObjectId(datum)));
	aclresult = pg_proc_aclcheck(oid, GetUserId(),
								 ACL_EXECUTE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_PROC,
					   get_namespace_name(DatumGetObjectId(datum)));


	datum = SysCacheGetAttr(PROCOID, procTuple,
							Anum_pg_proc_prorettype,
							&isNull);
	Assert(!isNull);
	if (DatumGetObjectId(datum) != BOOLOID)
		ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
						errmsg("callback function must return a boolean value"),
						errhint("The function returns \"%s\"", get_type_name(DatumGetObjectId(datum)))));


	datum = SysCacheGetAttr(PROCOID, procTuple,
							Anum_pg_proc_proallargtypes,
							&isNull);
	if (!isNull)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("callback function must not have any OUT parameters")));

	datum = SysCacheGetAttr(PROCOID, procTuple,
							Anum_pg_proc_proretset,
							&isNull);
	Assert(!isNull);
	if (DatumGetBool(datum))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("callback function must not return a set")));

	datum = SysCacheGetAttr(PROCOID, procTuple,
							Anum_pg_proc_proisagg,
							&isNull);
	Assert(!isNull);
	if (DatumGetBool(datum))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("callback function must not be an aggregate")));

	datum = SysCacheGetAttr(PROCOID, procTuple,
							Anum_pg_proc_proiswindow,
							&isNull);
	Assert(!isNull);
	if (DatumGetBool(datum))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("callback function must not be a window function")));

	/* the function should be OK, copy out the input argument types */

	nargs = procStruct->proargtypes.dim1;
	Assert(nargs == procStruct->pronargs);
	argtypes = (Oid *) palloc(sizeof(Oid) * nargs);
	memcpy(argtypes, procStruct->proargtypes.values, nargs * sizeof(Oid));

	/* done with the proc entry */
	ReleaseSysCache(procTuple);

	/* the first argument should match the input type */
	if (inputtype != argtypes[0])
		ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
						errmsg("callback function's first argument must match the array type"),
						errhint("Function expects \"%s\", array is of type \"%s\"", get_type_name(argtypes[0]), get_type_name(inputtype))));

	/*
	 * Now extract all arguments we're going to pass to the callback function
	 * from the variadic array, and run them through their respective input
	 * functions to get the correct Datums.
	 */
	get_typlenbyvalalign(CSTRINGOID, &typlen, &typbyval, &typalign);
	deconstruct_array(callback_args, CSTRINGOID, typlen, typbyval, typalign,
					  &variadicargs, &variadicnulls, &n_variadic);

	if (n_variadic < nargs - 1)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_PARAMETER),
						errmsg("Not enough arguments for callback function"),
						errhint("%d arguments given, function expects %d", n_variadic, nargs - 1)));
	if (n_variadic > nargs - 1)
		ereport(ERROR, (errcode(ERRCODE_TOO_MANY_ARGUMENTS),
						errmsg("Too many arguments for callback function"),
						errhint("%d arguments given, function expects %d", n_variadic, nargs - 1)));


	flinfo = (FmgrInfo *) palloc(sizeof(FmgrInfo));
	fmgr_info(oid, flinfo);

	for (i = 1; i < nargs; ++i)
	{
		Oid			inputoid;
		Oid			param;

		getTypeInputInfo(argtypes[i], &inputoid, &param);

		/* remember to use i - 1 for variadic* */
		if (variadicnulls[i - 1])
			fcinfo->argnull[i] = true;
		else
		{
			fcinfo->arg[i] = OidFunctionCall3(inputoid, variadicargs[i - 1], param, -1);
			fcinfo->argnull[i] = false;
		}
	}

	/*
	 * If we're on 9.1 or later, set the collation for the callback function.
	 * Otherwise functions like lower() won't work.
	 */
#if PG_VERSION_NUM >= 90100
	InitFunctionCallInfoData(*fcinfo, flinfo, nargs+1, DEFAULT_COLLATION_OID, NULL, NULL);
#else
	InitFunctionCallInfoData(*fcinfo, flinfo, nargs+1, NULL, NULL);
#endif
}

/*
 * Given an oid, return a palloc'd copy of the name of the data type.
 */
static char *
get_type_name(Oid oid)
{
	HeapTuple	typetup;
	Form_pg_type typeform;
	char	   *result;

	if (oid == InvalidOid)
		elog(ERROR, "invalid type oid");

	typetup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(oid));
	if (!HeapTupleIsValid(typetup))
		elog(ERROR, "cache lookup failed for type %u", oid);

	typeform = (Form_pg_type) GETSTRUCT(typetup);
	result = pstrdup(NameStr(typeform->typname));

	ReleaseSysCache(typetup);

	return result;
}

/*
 * Check whether a specific array element is NULL
 *
 * nullbitmap: pointer to array's null bitmap (NULL if none)
 * offset: 0-based linear element number of array element
 */
static bool
array_get_isnull(const bits8 *nullbitmap, int offset)
{
	if (nullbitmap == NULL)
		return false;		   /* assume not null */
	if (nullbitmap[offset / 8] & (1 << (offset % 8)))
		return false;		   /* not null */
	return true;
}


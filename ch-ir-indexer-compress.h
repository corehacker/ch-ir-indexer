/*******************************************************************************
 *  Repository for C modules.
 *  Copyright (C) 2014 Sandeep Prakash
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 ******************************************************************************/

/*******************************************************************************
 * Copyright (c) 2014, Sandeep Prakash <123sandy@gmail.com>
 *
 * \file   ch-ir-indexer-compress.h
 *
 * \author sandeepprakash
 *
 * \date   Mar 12, 2014
 *
 * \brief  
 *
 ******************************************************************************/

#ifndef __CH_IR_INDEXER_COMPRESS_H__
#define __CH_IR_INDEXER_COMPRESS_H__

#ifdef  __cplusplus
extern  "C"
{
#endif

/********************************* CONSTANTS **********************************/

/*********************************** MACROS ***********************************/

/******************************** ENUMERATIONS ********************************/

/*********************** CLASS/STRUCTURE/UNION DATA TYPES *********************/

/***************************** FUNCTION PROTOTYPES ****************************/
CH_IR_RET_E ch_ir_bit_store_init(
   CH_IR_BIT_STORE_CTXT_X **ppx_bit_store_ctxt);

CH_IR_RET_E ch_ir_bit_store_deinit(
   CH_IR_BIT_STORE_CTXT_X *px_bit_store_ctxt);

CH_IR_RET_E ch_ir_bit_store_write_bit(
   CH_IR_BIT_STORE_CTXT_X *px_bit_store_ctxt,
   bool b_bit_value);

CH_IR_RET_E ch_ir_compress_index (
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt);

#ifdef   __cplusplus
}
#endif

#endif /* __CH_IR_INDEXER_COMPRESS_H__ */

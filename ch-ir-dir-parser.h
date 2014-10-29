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
 * \file   ch-ir-dir-parser.h
 *
 * \author sandeepprakash
 *
 * \date   Mar 11, 2014
 *
 * \brief  
 *
 ******************************************************************************/

#ifndef __CH_IR_DIR_PARSER_H__
#define __CH_IR_DIR_PARSER_H__

#ifdef  __cplusplus
extern  "C"
{
#endif

/********************************* CONSTANTS **********************************/

/*********************************** MACROS ***********************************/
#define CH_IR_DIR_PARSER_MAX_FILENAME_LEN                           (16384)

/******************************** ENUMERATIONS ********************************/

/*********************** CLASS/STRUCTURE/UNION DATA TYPES *********************/
typedef CH_IR_RET_E (*pfn_ch_ir_for_each_file_cbk)(
   uint8_t *puc_file_path,
   uint32_t ui_file_idx,
   void *p_app_data);

typedef struct _CH_IR_DIR_PARSER_INIT_PARAMS_X
{
   uint32_t ui_max_filepath_len;

   pfn_ch_ir_for_each_file_cbk fn_ch_ir_for_each_file_cbk;

   void *p_app_data;
} CH_IR_DIR_PARSER_INIT_PARAMS_X;

typedef struct _CH_IR_DIR_PARSER_CTXT_X
{
   CH_IR_DIR_PARSER_INIT_PARAMS_X x_init_params;
} CH_IR_DIR_PARSER_CTXT_X;

/***************************** FUNCTION PROTOTYPES ****************************/
CH_IR_RET_E ch_ir_dir_parser_init(
   CH_IR_DIR_PARSER_INIT_PARAMS_X *px_init_params,
   CH_IR_DIR_PARSER_CTXT_X **ppx_dir_parser_ctxt);

CH_IR_RET_E ch_ir_dir_parser_deinit(
   CH_IR_DIR_PARSER_CTXT_X *px_dir_parser_ctxt);

CH_IR_RET_E ch_ir_dir_parser_parse_dir_for_files (
   CH_IR_DIR_PARSER_CTXT_X *px_dir_parser_ctxt,
   uint8_t *puc_dir_path);

#ifdef   __cplusplus
}
#endif

#endif /* __CH_IR_DIR_PARSER_H__ */

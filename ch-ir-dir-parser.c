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
 * \file   ch-ir-dir-parser.c
 *
 * \author sandeepprakash
 *
 * \date   Mar 11, 2014
 *
 * \brief  
 *
 ******************************************************************************/

/********************************** INCLUDES **********************************/
#include <ch-pal/exp_pal.h>
#include "ch-ir-common.h"
#include "ch-ir-dir-parser.h"

/********************************* CONSTANTS **********************************/

/*********************************** MACROS ***********************************/

/******************************** ENUMERATIONS ********************************/

/************************* STRUCTURE/UNION DATA TYPES *************************/

/************************ STATIC FUNCTION PROTOTYPES **************************/

/****************************** LOCAL FUNCTIONS *******************************/
CH_IR_RET_E ch_ir_dir_parser_init(
   CH_IR_DIR_PARSER_INIT_PARAMS_X *px_init_params,
   CH_IR_DIR_PARSER_CTXT_X **ppx_dir_parser_ctxt)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   CH_IR_DIR_PARSER_CTXT_X *px_dir_parser_ctxt = NULL;

   if ((NULL == px_init_params) || (NULL == ppx_dir_parser_ctxt))
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   if ((NULL == px_init_params->fn_ch_ir_for_each_file_cbk)
      || (0 == px_init_params->ui_max_filepath_len))
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   px_dir_parser_ctxt = pal_malloc (sizeof(CH_IR_DIR_PARSER_CTXT_X), NULL);
   if (NULL == px_dir_parser_ctxt)
   {
      CH_IR_LOG_MED("pal_malloc failed");
      e_ret_val = eCH_IR_RET_RESOURCE_FAILURE;
      goto CLEAN_RETURN;
   }

   (void) pal_memmove(&(px_dir_parser_ctxt->x_init_params), px_init_params,
      sizeof(px_dir_parser_ctxt->x_init_params));
   *ppx_dir_parser_ctxt = px_dir_parser_ctxt;
   e_ret_val = eCH_IR_RET_SUCCESS;
CLEAN_RETURN:
   return e_ret_val;
}

CH_IR_RET_E ch_ir_dir_parser_deinit(
   CH_IR_DIR_PARSER_CTXT_X *px_dir_parser_ctxt)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;

   if (NULL == px_dir_parser_ctxt)
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   pal_free (px_dir_parser_ctxt);
   px_dir_parser_ctxt = NULL;
   e_ret_val = eCH_IR_RET_SUCCESS;

CLEAN_RETURN:
   return e_ret_val;
}

CH_IR_RET_E ch_ir_dir_parser_parse_dir_for_files (
   CH_IR_DIR_PARSER_CTXT_X *px_dir_parser_ctxt,
   uint8_t *puc_dir_path)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   DIR *px_dir = NULL;
   struct dirent *px_dirent = NULL;
   uint8_t *puc_filename = NULL;
   uint32_t ui_file_idx = 0;

   if ((NULL == px_dir_parser_ctxt) || (NULL == puc_dir_path))
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   puc_filename = pal_malloc (
      px_dir_parser_ctxt->x_init_params.ui_max_filepath_len, NULL);
   if (NULL == puc_filename)
   {
      CH_IR_LOG_MED("pal_malloc failed");
      e_ret_val = eCH_IR_RET_RESOURCE_FAILURE;
      goto CLEAN_RETURN;
   }

   px_dir = opendir ((const char *) puc_dir_path);
   if (NULL != px_dir)
   {
      while ((px_dirent = readdir (px_dir)) != NULL)
      {
         if (8 == px_dirent->d_type)
         {
            ui_file_idx++;

            (void) pal_memset (puc_filename, 0x00,
               px_dir_parser_ctxt->x_init_params.ui_max_filepath_len);

            snprintf ((char *) puc_filename,
               px_dir_parser_ctxt->x_init_params.ui_max_filepath_len, "%s/%s",
               (const char *) puc_dir_path, px_dirent->d_name);

            px_dir_parser_ctxt->x_init_params.fn_ch_ir_for_each_file_cbk (
               puc_filename, ui_file_idx,
               px_dir_parser_ctxt->x_init_params.p_app_data);
         }
      }

      (void) closedir (px_dir);
      e_ret_val = eCH_IR_RET_SUCCESS;
   }
   else
   {
      CH_IR_LOG_MED("opendir failed");
      e_ret_val = eCH_IR_RET_RESOURCE_FAILURE;
   }
CLEAN_RETURN:
   if (NULL != puc_filename)
   {
      pal_free (puc_filename);
      puc_filename = NULL;
   }
   return e_ret_val;
}

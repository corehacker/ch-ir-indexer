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
 * \file   ch-ir-tokenizer.c
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
#include "ch-ir-tokenizer.h"

/********************************* CONSTANTS **********************************/

/*********************************** MACROS ***********************************/

/******************************** ENUMERATIONS ********************************/

/************************* STRUCTURE/UNION DATA TYPES *************************/

/************************ STATIC FUNCTION PROTOTYPES **************************/
static bool ch_ir_tokenizer_does_token_contain_only_numerals(
   uint8_t *puc_token);

static CH_IR_RET_E ch_ir_tokenizer_handle_token(
   CH_IR_TOKENIZER_CTXT_X *px_tokenizer_ctxt,
   uint8_t *puc_token,
   uint8_t *puc_file_path,
   uint32_t ui_file_idx);

static CH_IR_RET_E ch_ir_tokenizer_parse_line(
   CH_IR_TOKENIZER_CTXT_X *px_tokenizer_ctxt,
   uint8_t *puc_line,
   uint8_t *puc_file_path,
   uint32_t ui_file_idx);

static CH_IR_RET_E ch_ir_tokenizer_parse_file(
   CH_IR_TOKENIZER_CTXT_X *px_tokenizer_ctxt,
   uint8_t *puc_file_path,
   uint32_t ui_file_idx);

/****************************** LOCAL FUNCTIONS *******************************/
CH_IR_RET_E ch_ir_tokenizer_init(
   CH_IR_TOKENIZER_INIT_PARAMS_X *px_init_params,
   CH_IR_TOKENIZER_CTXT_X **ppx_tokenizer_ctxt)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   CH_IR_TOKENIZER_CTXT_X *px_tokenizer_ctxt = NULL;

   if ((NULL == px_init_params) || (NULL == ppx_tokenizer_ctxt))
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   if ((NULL == px_init_params->fn_ch_ir_for_each_token_cbk)
      || (0 == px_init_params->ui_max_token_len))
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   px_tokenizer_ctxt = pal_malloc (sizeof(CH_IR_TOKENIZER_CTXT_X), NULL);
   if (NULL == px_tokenizer_ctxt)
   {
      CH_IR_LOG_MED("pal_malloc failed");
      e_ret_val = eCH_IR_RET_RESOURCE_FAILURE;
      goto CLEAN_RETURN;
   }

   (void) pal_memmove(&(px_tokenizer_ctxt->x_init_params), px_init_params,
      sizeof(px_tokenizer_ctxt->x_init_params));
   *ppx_tokenizer_ctxt = px_tokenizer_ctxt;
   e_ret_val = eCH_IR_RET_SUCCESS;
CLEAN_RETURN:
   return e_ret_val;
}

CH_IR_RET_E ch_ir_tokenizer_deinit(
   CH_IR_TOKENIZER_CTXT_X *px_tokenizer_ctxt)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;

   if (NULL == px_tokenizer_ctxt)
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   pal_free (px_tokenizer_ctxt);
   px_tokenizer_ctxt = NULL;
   e_ret_val = eCH_IR_RET_SUCCESS;

CLEAN_RETURN:
   return e_ret_val;
}

static bool ch_ir_tokenizer_does_token_contain_only_numerals(
   uint8_t *puc_token)
{
   bool b_only_numerals = false;
   uint8_t uc_c = 0;
   uint32_t ui_i = 0;

   if (NULL == puc_token)
   {
      CH_IR_LOG_MED("Invalid Args");
      goto CLEAN_RETURN;
   }

   ui_i = pal_strlen (puc_token) - 1;

   while (ui_i > 0)
   {
      uc_c = puc_token [ui_i];

      if (uc_c < '0' || uc_c > '9')
      {
         break;
      }

      ui_i--;
   }

   if (0 == ui_i)
   {
      b_only_numerals = true;
   }

CLEAN_RETURN:
   return b_only_numerals;
}

static CH_IR_RET_E ch_ir_tokenizer_handle_token(
   CH_IR_TOKENIZER_CTXT_X *px_tokenizer_ctxt,
   uint8_t *puc_token,
   uint8_t *puc_file_path,
   uint32_t ui_file_idx)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;

   if ((NULL == px_tokenizer_ctxt) || (NULL == puc_token)
      || (NULL == puc_file_path))
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   px_tokenizer_ctxt->x_stats.ui_token_count++;

   e_ret_val = px_tokenizer_ctxt->x_init_params.fn_ch_ir_for_each_token_cbk (
      puc_token, puc_file_path, ui_file_idx,
      px_tokenizer_ctxt->x_init_params.p_app_data);
   if (eCH_IR_RET_SUCCESS != e_ret_val)
   {
      CH_IR_LOG_MED(
         "px_tokenizer_ctxt->x_init_params.fn_ch_ir_for_each_token_cbk failed: "
         "%d", e_ret_val);
   }
CLEAN_RETURN:
   return e_ret_val;
}

static CH_IR_RET_E ch_ir_tokenizer_parse_line(
   CH_IR_TOKENIZER_CTXT_X *px_tokenizer_ctxt,
   uint8_t *puc_line,
   uint8_t *puc_file_path,
   uint32_t ui_file_idx)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   uint8_t uc_c = 0;
   uint32_t ui_i = 0;
   bool b_ignore = false;
   bool b_break = false;
   uint8_t *puc_token = NULL;
   uint32_t ui_token_len = 0;
   bool b_only_numerals = false;

   if ((NULL == px_tokenizer_ctxt) || (NULL == puc_line)
      || (NULL == puc_file_path))
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   puc_token = pal_malloc (px_tokenizer_ctxt->x_init_params.ui_max_token_len,
      NULL);
   if (NULL == puc_token)
   {
      CH_IR_LOG_MED("pal_malloc failed");
      e_ret_val = eCH_IR_RET_RESOURCE_FAILURE;
      goto CLEAN_RETURN;
   }

   while (1)
   {
      uc_c = puc_line [ui_i];

      switch (uc_c)
      {
         case '\0':
         {
            if (ui_token_len > 0)
            {
               puc_token [ui_token_len] = '\0';

               if ('\'' == puc_token [0] && '\'' == puc_token [ui_token_len - 1])
               {
                  pal_memmove (puc_token, &puc_token [1], ui_token_len - 2);
                  puc_token [ui_token_len - 2] = '\0';
               }

               ch_ir_tokenizer_handle_token (px_tokenizer_ctxt, puc_token,
                  puc_file_path, ui_file_idx);

               (void) pal_memset (puc_token, 0x00,
                  px_tokenizer_ctxt->x_init_params.ui_max_token_len);
               ui_token_len = 0;
            }
            b_break = true;
            break;
         }
         case '<':
         {
            b_ignore = true;
            break;
         }
         case '>':
         {
            b_ignore = true;
            break;
         }
         case '.':
         {
            if (ui_token_len > 0)
            {
               puc_token [ui_token_len] = '\0';

               b_only_numerals = ch_ir_tokenizer_does_token_contain_only_numerals (puc_token);
               if ((true == b_only_numerals) && ('\0' != puc_line [ui_i + 1])
                  && ((puc_line [ui_i + 1] >= '0') && (puc_line [ui_i + 1] <= '9')))
               {
                  /*
                   * Handle the following case:
                   *    1. 10.901
                   */
                  puc_token [ui_token_len] = '.';
                  ui_token_len++;
               }
               else
               {

                  ch_ir_tokenizer_handle_token (px_tokenizer_ctxt, puc_token,
                     puc_file_path, ui_file_idx);

                  (void) pal_memset (puc_token, 0x00,
                     px_tokenizer_ctxt->x_init_params.ui_max_token_len);
                  ui_token_len = 0;
               }
            }
            break;
         }
         case ',':
         case '!':
         case ' ':
         case '(':
         case ')':
         case '/':
            /*
             * All these are considered as delimiters.
             */
         {
            if (ui_token_len > 0)
            {
               puc_token [ui_token_len] = '\0';

               if ('\'' == puc_token [0] && '\'' == puc_token [ui_token_len - 1])
               {
                  pal_memmove (puc_token, &puc_token [1], ui_token_len - 3);
                  puc_token [ui_token_len - 2] = '\0';
               }

               ch_ir_tokenizer_handle_token (px_tokenizer_ctxt, puc_token,
                  puc_file_path, ui_file_idx);

               (void) pal_memset (puc_token, 0x00,
                  px_tokenizer_ctxt->x_init_params.ui_max_token_len);
               ui_token_len = 0;
            }
            break;
         }
         default:
         {
            if (true == b_ignore)
            {
               break;
            }
            else
            {
               puc_token [ui_token_len] = tolower (uc_c);
               ui_token_len++;
            }
         }
      }

      if (true == b_break)
      {
         break;
      }

      ui_i++;
   }
   e_ret_val = eCH_IR_RET_SUCCESS;
CLEAN_RETURN:
   if (NULL != puc_token)
   {
      pal_free (puc_token);
      puc_token = NULL;
   }
   return e_ret_val;
}

static CH_IR_RET_E ch_ir_tokenizer_parse_file(
   CH_IR_TOKENIZER_CTXT_X *px_tokenizer_ctxt,
   uint8_t *puc_file_path,
   uint32_t ui_file_idx)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   PAL_RET_E e_pal_ret = ePAL_RET_FAILURE;
   PAL_FILE_HDL hl_file_hdl = NULL;
   uint8_t uca_line [MAX_LINE_SIZE] = { 0 };
   uint32_t ui_line_len = 0;

   if ((NULL == px_tokenizer_ctxt) || (NULL == puc_file_path))
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   e_pal_ret = pal_fopen (&hl_file_hdl, puc_file_path, (const uint8_t *) "r");
   if (ePAL_RET_SUCCESS != e_pal_ret)
   {
      CH_IR_LOG_MED("pal_fopen failed: %d", e_pal_ret);
      goto CLEAN_RETURN;
   }

   while (1)
   {
      e_pal_ret = pal_freadline (hl_file_hdl, uca_line, sizeof(uca_line),
         &ui_line_len);
      if (ePAL_RET_FILE_EOF_REACHED == e_pal_ret)
      {
         e_ret_val = eCH_IR_RET_SUCCESS;
         break;
      }

      e_ret_val = ch_ir_tokenizer_parse_line (px_tokenizer_ctxt, uca_line,
         puc_file_path, ui_file_idx);
   }

   e_pal_ret = pal_fclose (hl_file_hdl);

CLEAN_RETURN:
   return e_ret_val;
}

CH_IR_RET_E ch_ir_tokenizer_parse_tokens_in_file (
   CH_IR_TOKENIZER_CTXT_X *px_tokenizer_ctxt,
   uint8_t *puc_file_path,
   uint32_t ui_file_idx)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;

   if ((NULL == px_tokenizer_ctxt) || (NULL == puc_file_path))
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   e_ret_val = ch_ir_tokenizer_parse_file (px_tokenizer_ctxt, puc_file_path,
      ui_file_idx);

CLEAN_RETURN:
   return e_ret_val;
}

CH_IR_RET_E ch_ir_tokenizer_get_stats(
   CH_IR_TOKENIZER_CTXT_X *px_tokenizer_ctxt,
   CH_IR_TOKENIZER_STATS_X *px_stats)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;

   if ((NULL == px_tokenizer_ctxt) || (NULL == px_stats))
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   (void) pal_memmove (px_stats, &(px_tokenizer_ctxt->x_stats),
      sizeof(*px_stats));
   e_ret_val = eCH_IR_RET_SUCCESS;
CLEAN_RETURN:
   return e_ret_val;
}

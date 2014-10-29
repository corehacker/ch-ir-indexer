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
 * \file   ch-ir-indexer-pvt.c
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
#include <ch-utils/exp_list.h>
#include <ch-utils/exp_hashmap.h>
#include "ch-ir-common.h"
#include "ch-ir-dir-parser.h"
#include "ch-ir-tokenizer.h"
#include "ch-ir-indexer.h"
#include "ch-ir-indexer-pvt.h"
#include "ch-ir-indexer-compress.h"
#include "porter.h"

/********************************* CONSTANTS **********************************/

/*********************************** MACROS ***********************************/

/******************************** ENUMERATIONS ********************************/

/************************* STRUCTURE/UNION DATA TYPES *************************/

/************************ STATIC FUNCTION PROTOTYPES **************************/
static uint32_t ui_top_30_count = 0;

/****************************** LOCAL FUNCTIONS *******************************/
void ch_ir_indexer_handle_posting_found_in_hm (
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt,
   CH_IR_POSTING_X *px_posting,
   uint8_t *puc_token,
   uint8_t *puc_file_path,
   uint32_t ui_file_idx)
{
   px_posting->ui_doc_id = ui_file_idx;
   px_posting->ui_term_freq++;
}

void ch_ir_indexer_handle_token_found_in_hm (
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt,
   CH_IR_TOKEN_HM_ENTRY_X *px_token_hm_entry,
   uint8_t *puc_token,
   uint8_t *puc_file_path,
   uint32_t ui_file_idx)
{
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;
   HM_INIT_PARAMS_X x_hm_init_params = { 0 };
   HM_NODE_DATA_X x_node_data = { eHM_KEY_TYPE_INVALID };
   CH_IR_POSTING_X *px_posting = NULL;
   uint32_t ui_hm_size = 0;

   px_token_hm_entry->ui_num_occurances++;

   if (NULL == px_token_hm_entry->x_postings.hl_posting_hm)
   {
      x_hm_init_params.e_hm_key_type = eHM_KEY_TYPE_STRING;
      x_hm_init_params.b_maintain_linked_list = true;
      x_hm_init_params.ui_linked_list_flags |=
         eHM_LINKED_LIST_FLAGS_BM_UNSORTED;
      x_hm_init_params.ui_hm_table_size =
         px_indexer_ctxt->x_init_params.ui_postings_hm_table_size;
      e_hm_ret = hm_create (&(px_token_hm_entry->x_postings.hl_posting_hm),
         &x_hm_init_params);
      if (eHM_RET_SUCCESS != e_hm_ret)
      {
         CH_IR_LOG_MED("hm_create failed: %d", e_hm_ret);
         goto CLEAN_RETURN;
      }
   }

   (void) pal_memset (&x_node_data, 0x00, sizeof(x_node_data));
   x_node_data.e_hm_key_type = eHM_KEY_TYPE_STRING;
   x_node_data.u_hm_key.puc_str_key = puc_file_path;
   e_hm_ret = hm_search_node (px_token_hm_entry->x_postings.hl_posting_hm,
      &x_node_data);
   if (eHM_RET_HM_NODE_FOUND == e_hm_ret)
   {
      px_posting = (CH_IR_POSTING_X *) x_node_data.p_data;
      ch_ir_indexer_handle_posting_found_in_hm (px_indexer_ctxt, px_posting,
         puc_token, puc_file_path, ui_file_idx);
   }
   else
   {
      e_hm_ret = hm_get_total_count (
         px_token_hm_entry->x_postings.hl_posting_hm, &ui_hm_size);
      if (ui_hm_size > 0)
      {
         (void) pal_memset (&x_node_data, 0x00, sizeof(x_node_data));
         e_hm_ret = hm_linked_list_peek_at_tail (
            px_token_hm_entry->x_postings.hl_posting_hm, &x_node_data);
         if ((NULL != x_node_data.p_data) && (x_node_data.ui_data_size == sizeof(*px_posting)))
         {
            px_posting = (CH_IR_POSTING_X *) x_node_data.p_data;

            px_posting->ui_gap = ui_file_idx - px_posting->ui_doc_id;

            px_posting = NULL;
         }
      }
      px_posting = pal_malloc (sizeof(CH_IR_POSTING_X), NULL);

      (void) pal_memset (&x_node_data, 0x00, sizeof(x_node_data));
      x_node_data.e_hm_key_type = eHM_KEY_TYPE_STRING;
      x_node_data.u_hm_key.puc_str_key = puc_file_path;
      x_node_data.p_data = px_posting;
      x_node_data.ui_data_size = sizeof(*px_posting);
      e_hm_ret = hm_add_node (px_token_hm_entry->x_postings.hl_posting_hm,
         &x_node_data);
      if (eHM_RET_SUCCESS == e_hm_ret)
      {
         px_token_hm_entry->x_postings.ui_doc_freq++;
         ch_ir_indexer_handle_posting_found_in_hm (px_indexer_ctxt, px_posting,
            puc_token, puc_file_path, ui_file_idx);
      }
      else
      {

      }
   }

CLEAN_RETURN:
   return;
}

void ch_ir_indexer_handle_token_not_found_in_hm (
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt,
   uint8_t *puc_token,
   uint8_t *puc_file_path,
   uint32_t ui_file_idx)
{
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;
   HM_NODE_DATA_X x_node_data = { eHM_KEY_TYPE_INVALID };
   CH_IR_TOKEN_HM_ENTRY_X *px_token_hm_entry = NULL;
   uint32_t ui_token_len = 0;
   HM_INIT_PARAMS_X x_hm_init_params = {0};

   px_token_hm_entry = pal_malloc (sizeof(CH_IR_TOKEN_HM_ENTRY_X), NULL);

   ui_token_len = pal_strlen (puc_token) + 1;
   px_token_hm_entry->puc_token = pal_malloc (ui_token_len, NULL);

   pal_strncpy (px_token_hm_entry->puc_token, puc_token, ui_token_len);
   // px_token_hm_entry->ui_num_occurances++;
   //printf ("Count: %d\n", px_token_hm_entry->ui_num_occurances);

   (void) pal_memset (&x_node_data, 0x00, sizeof(x_node_data));
   x_node_data.e_hm_key_type = eHM_KEY_TYPE_STRING;
   x_node_data.u_hm_key.puc_str_key = puc_token;
   x_node_data.p_data = px_token_hm_entry;
   x_node_data.ui_data_size = sizeof(*px_token_hm_entry);
   e_hm_ret = hm_add_node (px_indexer_ctxt->hl_token_hm,
      &x_node_data);
   if (eHM_RET_SUCCESS == e_hm_ret)
   {
      ch_ir_indexer_handle_token_found_in_hm (px_indexer_ctxt,
         px_token_hm_entry, puc_token, puc_file_path, ui_file_idx);
   }
   else
   {
      printf ("Key \"%s\" Add failed: %p\n", puc_token, x_node_data.p_data);
   }

CLEAN_RETURN:
   return;
}

void ch_ir_indexer_handle_token(
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt,
   uint8_t *puc_token,
   uint8_t *puc_file_path,
   uint32_t ui_file_idx)
{
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;
   HM_NODE_DATA_X x_node_data = { eHM_KEY_TYPE_INVALID };
   CH_IR_TOKEN_HM_ENTRY_X *px_token_hm_entry = NULL;

   if (true == px_indexer_ctxt->x_init_params.b_ignore_stopwords)
   {
      (void) pal_memset (&x_node_data, 0x00, sizeof(x_node_data));
      x_node_data.e_hm_key_type = eHM_KEY_TYPE_STRING;
      x_node_data.u_hm_key.puc_str_key = puc_token;
      e_hm_ret = hm_search_node (px_indexer_ctxt->hl_stopword_hm, &x_node_data);
      if (eHM_RET_HM_NODE_FOUND == e_hm_ret)
      {
         // printf ("Key \"%s\" is a stop word. Ignoring!\n", token);
         px_indexer_ctxt->x_stats.ui_num_tokens_ignored++;
         goto LBL_CLEANUP;
      }
   }

   puc_token = (uint8_t *) porter_stem((char *) puc_token);

   (void) pal_memset (&x_node_data, 0x00, sizeof(x_node_data));
   x_node_data.e_hm_key_type = eHM_KEY_TYPE_STRING;
   x_node_data.u_hm_key.puc_str_key = puc_token;
   e_hm_ret = hm_search_node (px_indexer_ctxt->hl_token_hm, &x_node_data);
   if (eHM_RET_HM_NODE_FOUND == e_hm_ret)
   {
      px_token_hm_entry = (CH_IR_TOKEN_HM_ENTRY_X *) x_node_data.p_data;
      ch_ir_indexer_handle_token_found_in_hm (px_indexer_ctxt,
         px_token_hm_entry, puc_token, puc_file_path, ui_file_idx);
   }
   else
   {
      ch_ir_indexer_handle_token_not_found_in_hm (px_indexer_ctxt, puc_token,
         puc_file_path, ui_file_idx);
   }
LBL_CLEANUP:
   return;
}

CH_IR_RET_E ch_ir_indexer_for_each_token_cbk (
   uint8_t *puc_token,
   uint8_t *puc_file_path,
   uint32_t ui_file_idx,
   void *p_app_data)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt = NULL;

   if ((NULL == puc_token) || (NULL == puc_file_path) || (NULL == p_app_data))
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   px_indexer_ctxt = (CH_IR_INDEXER_CTXT_X *) p_app_data;

   // printf ("File: %s (%d), Token: %s\n", puc_file_path, ui_file_idx, puc_token);
   ch_ir_indexer_handle_token (px_indexer_ctxt, puc_token, puc_file_path,
      ui_file_idx);
   e_ret_val = eCH_IR_RET_SUCCESS;
CLEAN_RETURN:
   return e_ret_val;
}

CH_IR_RET_E ch_ir_indexer_for_each_file_cbk (
   uint8_t *puc_file_path,
   uint32_t ui_file_idx,
   void *p_app_data)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt = NULL;

   if ((NULL == puc_file_path) || (NULL == p_app_data))
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   px_indexer_ctxt = (CH_IR_INDEXER_CTXT_X *) p_app_data;

   // printf ("File: %s (%d)\n", puc_file_path, ui_file_idx);

   e_ret_val = ch_ir_tokenizer_parse_tokens_in_file (
      px_indexer_ctxt->px_tokenizer_ctxt, puc_file_path, ui_file_idx);
CLEAN_RETURN:
   return e_ret_val;
}

HM_RET_E ch_ir_indexer_token_hm_compare_cbk(
   HM_NODE_DATA_X *px_node_a_data,
   HM_NODE_DATA_X *px_node_b_data,
   void *p_app_data)
{
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;
   CH_IR_TOKEN_HM_ENTRY_X *px_app_node_a_data = NULL;
   CH_IR_TOKEN_HM_ENTRY_X *px_app_node_b_data = NULL;

   if ((NULL == px_node_b_data) || (NULL == px_node_a_data))
   {
      printf ("Invalid Args");
      e_hm_ret = eHM_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   px_app_node_a_data = (CH_IR_TOKEN_HM_ENTRY_X *) px_node_a_data->p_data;
   px_app_node_b_data = (CH_IR_TOKEN_HM_ENTRY_X *) px_node_b_data->p_data;

   if (px_app_node_a_data->ui_num_occurances
      < px_app_node_b_data->ui_num_occurances)
   {
      e_hm_ret = eHM_RET_CMP_GREATER;
   }
   else if (px_app_node_a_data->ui_num_occurances
      > px_app_node_b_data->ui_num_occurances)
   {
      e_hm_ret = eHM_RET_CMP_LESSER;
   }
   else
   {
      e_hm_ret = eHM_RET_CMP_EQUAL;
   }

CLEAN_RETURN:
   return e_hm_ret;
}

HM_RET_E ch_ir_indexer_postings_hm_for_each_cbk(
   HM_NODE_DATA_X *px_curr_node_data,
   void *p_app_data)
{
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;
   CH_IR_POSTING_X *px_posting = NULL;
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt = NULL;

   px_indexer_ctxt = (CH_IR_INDEXER_CTXT_X *) p_app_data;
   px_posting = (CH_IR_POSTING_X *) px_curr_node_data->p_data;

   printf ("(%d,%d,%d), ",
      px_posting->ui_doc_id, px_posting->ui_term_freq, px_posting->ui_gap);
   e_hm_ret = eHM_RET_SUCCESS;
LBL_CLEANUP:
   return e_hm_ret;
}

HM_RET_E ch_ir_indexer_token_hm_for_each_cbk(
   HM_NODE_DATA_X *px_curr_node_data,
   void *p_app_data)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;
   CH_IR_TOKEN_HM_ENTRY_X *px_token_hm_entry = NULL;
   CH_IR_TOKEN_HM_ENTRY_X *px_token_hm_entry_list = NULL;
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt = NULL;
   double d_frequency = 0.0;
   HM_FOR_EACH_PARAMS_X x_for_each_param = {eHM_FOR_EACH_DIRECTION_INVALID};
   uint32_t ui_total_tokens = 0;

   px_indexer_ctxt = (CH_IR_INDEXER_CTXT_X *) p_app_data;
   px_token_hm_entry = (CH_IR_TOKEN_HM_ENTRY_X *) px_curr_node_data->p_data;

   if (1 == px_token_hm_entry->ui_num_occurances)
   {
      px_indexer_ctxt->x_stats.ui_num_tokens_occuring_once++;
   }

   ui_top_30_count++;

   if (31 == ui_top_30_count)
   {
      ui_top_30_count = 0;
      e_hm_ret = eLIST_RET_FAILURE;
   }
   else
   {
      d_frequency = (((double) px_token_hm_entry->ui_num_occurances
         / (double) px_indexer_ctxt->x_stats.x_tokenizer_stats.ui_token_count)
         * (double) 100);
      printf ("| %7d | %30s | %10d | %7.4lf%% | ", ui_top_30_count,
         px_token_hm_entry->puc_token, px_token_hm_entry->ui_num_occurances,
         d_frequency);

      x_for_each_param.e_data_structure = eHM_DATA_STRUCT_LINKED_LIST;
      x_for_each_param.e_direction = eHM_FOR_EACH_DIRECTION_FORWARD;
      e_hm_ret = hm_for_each_v2 (px_token_hm_entry->x_postings.hl_posting_hm,
         &x_for_each_param, ch_ir_indexer_postings_hm_for_each_cbk,
         px_indexer_ctxt);
      printf ("\n");
      e_hm_ret = eHM_RET_SUCCESS;
   }
LBL_CLEANUP:
   return e_hm_ret;
}

void ch_ir_indexer_create_stopwords_cache_hm (
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt,
   uint8_t *puc_stopwords_filepath)
{
   PAL_FILE_HDL hl_file_hdl = NULL;
   PAL_RET_E e_pal_ret = ePAL_RET_FAILURE;
   uint32_t ui_line_count = 0;
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;
   HM_INIT_PARAMS_X x_hm_init_params = { 0 };
   uint32_t ui_line_len = 0;
   uint32_t ui_actual_line_len = 0;
   uint8_t *puc_line = NULL;
   uint32_t ui_max_token_len = 0;
   HM_NODE_DATA_X x_node_data = { eHM_KEY_TYPE_INVALID };

   e_pal_ret = pal_fopen (&hl_file_hdl, puc_stopwords_filepath,
      (const uint8_t *) "r");
   if ((ePAL_RET_SUCCESS == e_pal_ret) && (NULL != hl_file_hdl))
   {
      e_pal_ret = pal_fget_line_count (hl_file_hdl, &ui_line_count);
      if ((ePAL_RET_SUCCESS == e_pal_ret) && (0 != ui_line_count))
      {
         x_hm_init_params.ui_hm_table_size = ui_line_count;
      }
      else
      {
         x_hm_init_params.ui_hm_table_size =
            DEFAULT_STOPWORDS_HASHMAP_TABLE_SIZE;
      }
   }
   else
   {
      x_hm_init_params.ui_hm_table_size = DEFAULT_STOPWORDS_HASHMAP_TABLE_SIZE;
   }

   x_hm_init_params.e_hm_key_type = eHM_KEY_TYPE_STRING;
   e_hm_ret = hm_create (&(px_indexer_ctxt->hl_stopword_hm), &x_hm_init_params);
   if (eHM_RET_SUCCESS != e_hm_ret)
   {
      printf ("hm_create failed: %d\n", e_hm_ret);
      goto LBL_CLEANUP;
   }

   e_pal_ret = pal_frewind(hl_file_hdl);
   if (eHM_RET_SUCCESS != e_hm_ret)
   {
      printf ("pal_frewind failed: %d\n", e_pal_ret);
      goto LBL_CLEANUP;
   }

   ui_max_token_len = px_indexer_ctxt->x_init_params.ui_max_token_len;
   puc_line = pal_malloc (ui_max_token_len, NULL);

   while (1)
   {
      (void) pal_memset(puc_line, 0x00, ui_max_token_len);
      e_pal_ret = pal_freadline_v2 (hl_file_hdl, puc_line, ui_max_token_len,
         &ui_line_len, &ui_actual_line_len);
      if (ePAL_RET_FILE_EOF_REACHED == e_pal_ret)
      {
         e_pal_ret = ePAL_RET_SUCCESS;
         break;
      }
      else
      {
         if (ePAL_RET_FILE_READ_BUF_OVERFLOW == e_pal_ret)
         {
            pal_free (puc_line);
            puc_line = NULL;
            ui_max_token_len = ui_actual_line_len;
            puc_line = pal_malloc (ui_max_token_len, NULL);
            e_pal_ret = pal_frewind(hl_file_hdl);
            if (eHM_RET_SUCCESS != e_hm_ret)
            {
               break;
            }
            else
            {
               continue;
            }
         }
         else if (ePAL_RET_SUCCESS != e_pal_ret)
         {
            PAL_LOG_HIGH("pal_freadline failed: %d", e_pal_ret);
            break;
         }
         else
         {
            (void) pal_memset (&x_node_data, 0x00, sizeof(x_node_data));
            x_node_data.e_hm_key_type = eHM_KEY_TYPE_STRING;
            x_node_data.u_hm_key.puc_str_key = puc_line;
            e_hm_ret = hm_search_node (px_indexer_ctxt->hl_stopword_hm,
               &x_node_data);
            if (eHM_RET_HM_NODE_FOUND != e_hm_ret)
            {
               (void) pal_memset (&x_node_data, 0x00, sizeof(x_node_data));
               x_node_data.e_hm_key_type = eHM_KEY_TYPE_STRING;
               x_node_data.u_hm_key.puc_str_key = puc_line;
               x_node_data.p_data = NULL;
               x_node_data.ui_data_size = 0;
               e_hm_ret = hm_add_node (px_indexer_ctxt->hl_stopword_hm,
                  &x_node_data);
               if (eHM_RET_SUCCESS == e_hm_ret)
               {
                  //printf ("Key \"%s\" Added: %p\n", puc_line,
                  //   x_node_data.p_data);
               }
               else
               {
                  printf ("Key \"%s\" Add failed: %p\n", puc_line,
                     x_node_data.p_data);
                  break;
               }
            }
         }
      }
   }
LBL_CLEANUP:
   if (NULL != puc_line)
   {
      pal_free (puc_line);
      puc_line = NULL;
   }
   if (NULL != hl_file_hdl)
   {
      (void) pal_fclose(hl_file_hdl);
      hl_file_hdl = NULL;
   }
   return;
}

HM_RET_E ch_ir_indexer_postings_hm_for_each_delete_cbk(
   HM_NODE_DATA_X *px_curr_node_data,
   void *p_app_data)
{
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;
   CH_IR_POSTING_X *px_posting = NULL;
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt = NULL;

   px_indexer_ctxt = (CH_IR_INDEXER_CTXT_X *) p_app_data;
   px_posting = (CH_IR_POSTING_X *) px_curr_node_data->p_data;

   if (NULL != px_curr_node_data->p_data)
   {
      pal_free (px_curr_node_data->p_data);
      px_curr_node_data->p_data = NULL;
   }

   e_hm_ret = eHM_RET_SUCCESS;
LBL_CLEANUP:
   return e_hm_ret;
}

HM_RET_E ch_ir_indexer_token_hm_for_each_delete_cbk(
   HM_NODE_DATA_X *px_curr_node_data,
   void *p_app_data)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;
   CH_IR_TOKEN_HM_ENTRY_X *px_token_hm_entry = NULL;
   CH_IR_TOKEN_HM_ENTRY_X *px_token_hm_entry_list = NULL;
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt = NULL;
   HM_FOR_EACH_PARAMS_X x_for_each_param = {eHM_FOR_EACH_DIRECTION_INVALID};

   px_indexer_ctxt = (CH_IR_INDEXER_CTXT_X *) p_app_data;
   px_token_hm_entry = (CH_IR_TOKEN_HM_ENTRY_X *) px_curr_node_data->p_data;

   if (NULL != px_token_hm_entry->x_postings.hl_posting_hm)
   {
      x_for_each_param.e_data_structure = eHM_DATA_STRUCT_LINKED_LIST;
      x_for_each_param.e_direction = eHM_FOR_EACH_DIRECTION_FORWARD;
      e_hm_ret = hm_for_each_v2 (px_token_hm_entry->x_postings.hl_posting_hm,
         &x_for_each_param, ch_ir_indexer_postings_hm_for_each_delete_cbk,
         px_indexer_ctxt);

      e_hm_ret = hm_delete_all_nodes(px_token_hm_entry->x_postings.hl_posting_hm);

      e_hm_ret = hm_delete (px_token_hm_entry->x_postings.hl_posting_hm);
      px_token_hm_entry->x_postings.hl_posting_hm = NULL;
   }

   if (NULL != px_token_hm_entry->x_compressed_postings.px_doc_id)
   {
      (void) ch_ir_bit_store_deinit (
         px_token_hm_entry->x_compressed_postings.px_doc_id);
      px_token_hm_entry->x_compressed_postings.px_doc_id = NULL;
   }

   if (NULL != px_token_hm_entry->x_compressed_postings.px_term_freq)
   {
      (void) ch_ir_bit_store_deinit (
         px_token_hm_entry->x_compressed_postings.px_term_freq);
      px_token_hm_entry->x_compressed_postings.px_term_freq = NULL;
   }

   if (NULL != px_token_hm_entry->puc_token)
   {
      pal_free (px_token_hm_entry->puc_token);
      px_token_hm_entry->puc_token = NULL;
   }

   pal_free (px_token_hm_entry);
   px_token_hm_entry = NULL;

   e_hm_ret = eHM_RET_SUCCESS;
LBL_CLEANUP:
   return e_hm_ret;
}

HM_RET_E ch_ir_indexer_stopwords_hm_for_each_delete_cbk(
   HM_NODE_DATA_X *px_curr_node_data,
   void *p_app_data)
{
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;
   CH_IR_POSTING_X *px_posting = NULL;
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt = NULL;

   px_indexer_ctxt = (CH_IR_INDEXER_CTXT_X *) p_app_data;
   px_posting = (CH_IR_POSTING_X *) px_curr_node_data->p_data;

   if (NULL != px_curr_node_data->p_data)
   {
      pal_free (px_curr_node_data->p_data);
      px_curr_node_data->p_data = NULL;
   }

   e_hm_ret = eHM_RET_SUCCESS;
LBL_CLEANUP:
   return e_hm_ret;
}

CH_IR_RET_E ch_ir_indexer_delete_index (
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   HM_FOR_EACH_PARAMS_X x_for_each_param = {eHM_DATA_STRUCT_INVALID};
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;

   if (NULL == px_indexer_ctxt)
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   /*
    * TODO: Cleanup token hashmap.
    */
   x_for_each_param.e_data_structure = eHM_DATA_STRUCT_LINKED_LIST;
   x_for_each_param.e_direction = eHM_FOR_EACH_DIRECTION_FORWARD;
   e_hm_ret = hm_for_each_v2 (px_indexer_ctxt->hl_token_hm, &x_for_each_param,
      ch_ir_indexer_token_hm_for_each_delete_cbk, px_indexer_ctxt);

   e_hm_ret = hm_delete_all_nodes(px_indexer_ctxt->hl_token_hm);

   e_ret_val = eCH_IR_RET_SUCCESS;

CLEAN_RETURN:
   return e_ret_val;
}

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
 * \file   ch-ir-indexer-serialize.c
 *
 * \author sandeepprakash
 *
 * \date   Mar 12, 2014
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
#include "ch-ir-indexer-serialize.h"

/********************************* CONSTANTS **********************************/

/*********************************** MACROS ***********************************/

/******************************** ENUMERATIONS ********************************/

/************************* STRUCTURE/UNION DATA TYPES *************************/

/************************ STATIC FUNCTION PROTOTYPES **************************/
static HM_RET_E ch_ir_indexer_token_hm_for_each_serialize_uncompressed_cbk (
   HM_NODE_DATA_X *px_curr_node_data,
   void *p_app_data);

static HM_RET_E ch_ir_indexer_postings_hm_for_each_serialize_uncompressed_cbk(
   HM_NODE_DATA_X *px_curr_node_data,
   void *p_app_data);

static HM_RET_E ch_ir_indexer_token_hm_for_each_serialize_compressed_cbk (
   HM_NODE_DATA_X *px_curr_node_data,
   void *p_app_data);

static HM_RET_E ch_ir_indexer_postings_hm_for_each_serialize_compressed_cbk(
   HM_NODE_DATA_X *px_curr_node_data,
   void *p_app_data);

/****************************** LOCAL FUNCTIONS *******************************/
static HM_RET_E ch_ir_indexer_token_hm_for_each_serialize_uncompressed_cbk (
   HM_NODE_DATA_X *px_curr_node_data,
   void *p_app_data)
{
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;
   CH_IR_TOKEN_HM_ENTRY_X *px_token_hm_entry = NULL;
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt = NULL;
   uint32_t ui_token_len = 0;
   uint32_t ui_postings_len = 0;
   uint32_t ui_fwrite_len = 0;
   PAL_RET_E e_pal_ret = ePAL_RET_FAILURE;
   HM_FOR_EACH_PARAMS_X x_for_each_param = {eHM_FOR_EACH_DIRECTION_INVALID};

   px_indexer_ctxt = (CH_IR_INDEXER_CTXT_X *) p_app_data;
   px_token_hm_entry = (CH_IR_TOKEN_HM_ENTRY_X *) px_curr_node_data->p_data;

   ui_token_len = pal_strlen (px_token_hm_entry->puc_token);

   ui_fwrite_len = sizeof(ui_token_len);
   e_pal_ret = pal_fwrite (px_indexer_ctxt->hl_uncompressed_file,
      (uint8_t *) &ui_token_len, &ui_fwrite_len);
   if ((ePAL_RET_SUCCESS != e_pal_ret)
      || (ui_fwrite_len != sizeof(ui_token_len)))
   {
      CH_IR_LOG_MED("pal_fwrite failed: %d", e_pal_ret);
      e_hm_ret = eHM_RET_RESOURCE_FAILURE;
      goto CLEAN_RETURN;
   }

   px_indexer_ctxt->x_stats.ui_uncompressed_index_size_bytes += ui_fwrite_len;

   ui_fwrite_len = ui_token_len;

   if (ui_fwrite_len > 0)
   {
      e_pal_ret = pal_fwrite (px_indexer_ctxt->hl_uncompressed_file,
         px_token_hm_entry->puc_token, &ui_fwrite_len);
      if ((ePAL_RET_SUCCESS != e_pal_ret) || (ui_fwrite_len != ui_token_len))
      {
         CH_IR_LOG_MED("pal_fwrite failed: %d", e_pal_ret);
         e_hm_ret = eHM_RET_RESOURCE_FAILURE;
         goto CLEAN_RETURN;
      }
      px_indexer_ctxt->x_stats.ui_uncompressed_index_size_bytes += ui_fwrite_len;
   }
   e_hm_ret = hm_get_total_count(px_token_hm_entry->x_postings.hl_posting_hm,
      &ui_postings_len);

   ui_postings_len = ui_postings_len * sizeof(CH_IR_POSTING_X); // Calculate
   ui_fwrite_len = sizeof(ui_postings_len);
   e_pal_ret = pal_fwrite (px_indexer_ctxt->hl_uncompressed_file,
      (uint8_t *) &ui_postings_len, &ui_fwrite_len);
   if ((ePAL_RET_SUCCESS != e_pal_ret)
      || (ui_fwrite_len != sizeof(ui_postings_len)))
   {
      CH_IR_LOG_MED("pal_fwrite failed: %d", e_pal_ret);
      e_hm_ret = eHM_RET_RESOURCE_FAILURE;
      goto CLEAN_RETURN;
   }
   px_indexer_ctxt->x_stats.ui_uncompressed_index_size_bytes += ui_fwrite_len;

   x_for_each_param.e_data_structure = eHM_DATA_STRUCT_LINKED_LIST;
   x_for_each_param.e_direction = eHM_FOR_EACH_DIRECTION_FORWARD;
   e_hm_ret = hm_for_each_v2 (px_token_hm_entry->x_postings.hl_posting_hm,
      &x_for_each_param,
      ch_ir_indexer_postings_hm_for_each_serialize_uncompressed_cbk,
      px_indexer_ctxt);
   if (eHM_RET_SUCCESS != e_hm_ret)
   {
      CH_IR_LOG_MED("hm_for_each_v2 failed: %d", e_hm_ret);
   }
CLEAN_RETURN:
   return e_hm_ret;
}

static HM_RET_E ch_ir_indexer_postings_hm_for_each_serialize_uncompressed_cbk(
   HM_NODE_DATA_X *px_curr_node_data,
   void *p_app_data)
{
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;
   CH_IR_POSTING_X *px_posting = NULL;
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt = NULL;
   uint32_t ui_posting_len = 0;
   uint32_t ui_fwrite_len = 0;
   PAL_RET_E e_pal_ret = ePAL_RET_FAILURE;

   px_indexer_ctxt = (CH_IR_INDEXER_CTXT_X *) p_app_data;
   px_posting = (CH_IR_POSTING_X *) px_curr_node_data->p_data;

   ui_posting_len = sizeof(*px_posting);
   ui_fwrite_len = ui_posting_len;
   e_pal_ret = pal_fwrite (px_indexer_ctxt->hl_uncompressed_file,
      (uint8_t *) px_posting, &ui_fwrite_len);
   if ((ePAL_RET_SUCCESS != e_pal_ret) || (ui_fwrite_len != ui_posting_len))
   {
      CH_IR_LOG_MED("pal_fwrite failed: %d", e_pal_ret);
      e_hm_ret = eHM_RET_RESOURCE_FAILURE;
      goto CLEAN_RETURN;
   }
   else
   {
      px_indexer_ctxt->x_stats.ui_uncompressed_index_size_bytes += ui_fwrite_len;
      e_hm_ret = eHM_RET_SUCCESS;
   }
CLEAN_RETURN:
   return e_hm_ret;
}

CH_IR_RET_E ch_ir_indexer_serialize_uncompressed_index (
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt,
   uint8_t *puc_filename)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   PAL_RET_E e_pal_ret = ePAL_RET_FAILURE;
   HM_FOR_EACH_PARAMS_X x_for_each_param = {eHM_DATA_STRUCT_INVALID};
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;

   if ((NULL == px_indexer_ctxt) || (NULL == puc_filename))
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   e_pal_ret = pal_fopen (&(px_indexer_ctxt->hl_uncompressed_file),
      puc_filename, "wb");
   if ((ePAL_RET_SUCCESS != e_pal_ret)
      || (NULL == px_indexer_ctxt->hl_uncompressed_file))
   {
      CH_IR_LOG_MED("pal_fopen failed: %d", e_pal_ret);
      e_ret_val = eCH_IR_RET_RESOURCE_FAILURE;
      goto CLEAN_RETURN;
   }

   px_indexer_ctxt->x_stats.ui_uncompressed_index_size_bytes = 0;

   x_for_each_param.e_data_structure = eHM_DATA_STRUCT_LINKED_LIST;
   x_for_each_param.e_direction = eHM_FOR_EACH_DIRECTION_FORWARD;
   e_hm_ret = hm_for_each_v2 (px_indexer_ctxt->hl_token_hm, &x_for_each_param,
      ch_ir_indexer_token_hm_for_each_serialize_uncompressed_cbk,
      px_indexer_ctxt);

   pal_fclose(px_indexer_ctxt->hl_uncompressed_file);

CLEAN_RETURN:
   return e_ret_val;
}

static HM_RET_E ch_ir_indexer_token_hm_for_each_serialize_compressed_cbk (
   HM_NODE_DATA_X *px_curr_node_data,
   void *p_app_data)
{
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;
   CH_IR_TOKEN_HM_ENTRY_X *px_token_hm_entry = NULL;
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt = NULL;
   uint32_t ui_token_len = 0;
   uint32_t ui_postings_len = 0;
   uint32_t ui_fwrite_len = 0;
   PAL_RET_E e_pal_ret = ePAL_RET_FAILURE;
   HM_FOR_EACH_PARAMS_X x_for_each_param = {eHM_FOR_EACH_DIRECTION_INVALID};

   px_indexer_ctxt = (CH_IR_INDEXER_CTXT_X *) p_app_data;
   px_token_hm_entry = (CH_IR_TOKEN_HM_ENTRY_X *) px_curr_node_data->p_data;

   ui_token_len = pal_strlen (px_token_hm_entry->puc_token);
   ui_fwrite_len = sizeof(ui_token_len);
   e_pal_ret = pal_fwrite (px_indexer_ctxt->hl_compressed_file,
      (uint8_t *) &ui_token_len, &ui_fwrite_len);
   if ((ePAL_RET_SUCCESS != e_pal_ret)
      || (ui_fwrite_len != sizeof(ui_token_len)))
   {
      CH_IR_LOG_MED("pal_fwrite failed: %d", e_pal_ret);
      e_hm_ret = eHM_RET_RESOURCE_FAILURE;
      goto CLEAN_RETURN;
   }
   px_indexer_ctxt->x_stats.ui_compressed_index_size_bytes += ui_fwrite_len;

   ui_fwrite_len = ui_token_len;

   if (ui_fwrite_len > 0)
   {
      e_pal_ret = pal_fwrite (px_indexer_ctxt->hl_compressed_file,
         px_token_hm_entry->puc_token, &ui_fwrite_len);
      if ((ePAL_RET_SUCCESS != e_pal_ret) || (ui_fwrite_len != ui_token_len))
      {
         CH_IR_LOG_MED("pal_fwrite failed: %d", e_pal_ret);
         e_hm_ret = eHM_RET_RESOURCE_FAILURE;
         goto CLEAN_RETURN;
      }
      px_indexer_ctxt->x_stats.ui_compressed_index_size_bytes += ui_fwrite_len;
   }

   ui_fwrite_len =
      px_token_hm_entry->x_compressed_postings.px_doc_id->ui_cur_byte + 1;

   if (ui_fwrite_len > 0)
   {
      //printf ("Writing %d bytes\n", ui_fwrite_len);
      e_pal_ret = pal_fwrite (px_indexer_ctxt->hl_compressed_file,
         px_token_hm_entry->x_compressed_postings.px_doc_id->puc_data,
         &ui_fwrite_len);
      if ((ePAL_RET_SUCCESS != e_pal_ret)
         || (ui_fwrite_len
            != px_token_hm_entry->x_compressed_postings.px_doc_id->ui_cur_byte + 1))
      {
         CH_IR_LOG_MED("pal_fwrite failed: %d, %d, %d", e_pal_ret,
            ui_fwrite_len,
            px_token_hm_entry->x_compressed_postings.px_doc_id->ui_cur_byte);
         e_hm_ret = eHM_RET_RESOURCE_FAILURE;
         goto CLEAN_RETURN;
      }
      px_indexer_ctxt->x_stats.ui_compressed_index_size_bytes += ui_fwrite_len;
   }

   ui_fwrite_len =
      px_token_hm_entry->x_compressed_postings.px_term_freq->ui_cur_byte + 1;
   if (ui_fwrite_len > 0)
   {
      //printf ("Writing %d bytes\n", ui_fwrite_len);
      e_pal_ret = pal_fwrite (px_indexer_ctxt->hl_compressed_file,
         px_token_hm_entry->x_compressed_postings.px_term_freq->puc_data,
         &ui_fwrite_len);
      if ((ePAL_RET_SUCCESS != e_pal_ret)
         || (ui_fwrite_len
            != px_token_hm_entry->x_compressed_postings.px_term_freq->ui_cur_byte + 1))
      {
         CH_IR_LOG_MED("pal_fwrite failed: %d", e_pal_ret);
         e_hm_ret = eHM_RET_RESOURCE_FAILURE;
         goto CLEAN_RETURN;
      }
      px_indexer_ctxt->x_stats.ui_compressed_index_size_bytes += ui_fwrite_len;
   }

   e_hm_ret = hm_get_total_count(px_token_hm_entry->x_postings.hl_posting_hm,
      &ui_postings_len);

   ui_postings_len = ui_postings_len * sizeof(uint32_t); // Calculate
   ui_fwrite_len = sizeof(ui_postings_len);
   e_pal_ret = pal_fwrite (px_indexer_ctxt->hl_compressed_file,
      (uint8_t *) &ui_postings_len, &ui_fwrite_len);
   if ((ePAL_RET_SUCCESS != e_pal_ret)
      || (ui_fwrite_len != sizeof(ui_postings_len)))
   {
      CH_IR_LOG_MED("pal_fwrite failed: %d", e_pal_ret);
      e_hm_ret = eHM_RET_RESOURCE_FAILURE;
      goto CLEAN_RETURN;
   }
   px_indexer_ctxt->x_stats.ui_compressed_index_size_bytes += ui_fwrite_len;

   x_for_each_param.e_data_structure = eHM_DATA_STRUCT_LINKED_LIST;
   x_for_each_param.e_direction = eHM_FOR_EACH_DIRECTION_FORWARD;
   e_hm_ret = hm_for_each_v2 (px_token_hm_entry->x_postings.hl_posting_hm,
      &x_for_each_param,
      ch_ir_indexer_postings_hm_for_each_serialize_compressed_cbk,
      px_indexer_ctxt);
   if (eHM_RET_SUCCESS != e_hm_ret)
   {
      CH_IR_LOG_MED("hm_for_each_v2 failed: %d", e_hm_ret);
   }
CLEAN_RETURN:
   return e_hm_ret;
}

static HM_RET_E ch_ir_indexer_postings_hm_for_each_serialize_compressed_cbk(
   HM_NODE_DATA_X *px_curr_node_data,
   void *p_app_data)
{
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;
   CH_IR_POSTING_X *px_posting = NULL;
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt = NULL;
   uint32_t ui_posting_len = 0;
   uint32_t ui_fwrite_len = 0;
   PAL_RET_E e_pal_ret = ePAL_RET_FAILURE;

   px_indexer_ctxt = (CH_IR_INDEXER_CTXT_X *) p_app_data;
   px_posting = (CH_IR_POSTING_X *) px_curr_node_data->p_data;

   ui_posting_len = sizeof(uint32_t);
   ui_fwrite_len = ui_posting_len;
   e_pal_ret = pal_fwrite (px_indexer_ctxt->hl_compressed_file,
      (uint8_t *) &(px_posting->ui_gap), &ui_fwrite_len);
   if ((ePAL_RET_SUCCESS != e_pal_ret) || (ui_fwrite_len != ui_posting_len))
   {
      CH_IR_LOG_MED("pal_fwrite failed: %d", e_pal_ret);
      e_hm_ret = eHM_RET_RESOURCE_FAILURE;
      goto CLEAN_RETURN;
   }
   else
   {
      px_indexer_ctxt->x_stats.ui_compressed_index_size_bytes += ui_fwrite_len;
      e_hm_ret = eHM_RET_SUCCESS;
   }
CLEAN_RETURN:
   return e_hm_ret;
}

CH_IR_RET_E ch_ir_indexer_serialize_compressed_index (
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt,
   uint8_t *puc_filename)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   PAL_RET_E e_pal_ret = ePAL_RET_FAILURE;
   HM_FOR_EACH_PARAMS_X x_for_each_param = {eHM_DATA_STRUCT_INVALID};
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;

   if ((NULL == px_indexer_ctxt) || (NULL == puc_filename))
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   e_pal_ret = pal_fopen (&(px_indexer_ctxt->hl_compressed_file),
      puc_filename, "wb");
   if ((ePAL_RET_SUCCESS != e_pal_ret)
      || (NULL == px_indexer_ctxt->hl_compressed_file))
   {
      CH_IR_LOG_MED("pal_fopen failed: %d", e_pal_ret);
      e_ret_val = eCH_IR_RET_RESOURCE_FAILURE;
      goto CLEAN_RETURN;
   }

   px_indexer_ctxt->x_stats.ui_compressed_index_size_bytes = 0;

   x_for_each_param.e_data_structure = eHM_DATA_STRUCT_LINKED_LIST;
   x_for_each_param.e_direction = eHM_FOR_EACH_DIRECTION_FORWARD;
   e_hm_ret = hm_for_each_v2 (px_indexer_ctxt->hl_token_hm, &x_for_each_param,
      ch_ir_indexer_token_hm_for_each_serialize_compressed_cbk,
      px_indexer_ctxt);

   pal_fclose(px_indexer_ctxt->hl_compressed_file);

CLEAN_RETURN:
   return e_ret_val;
}

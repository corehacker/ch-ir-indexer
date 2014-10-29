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
 * \file   ch-ir-indexer-compress.c
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
#include "ch-ir-indexer-compress.h"

/********************************* CONSTANTS **********************************/

/*********************************** MACROS ***********************************/

/******************************** ENUMERATIONS ********************************/

/************************* STRUCTURE/UNION DATA TYPES *************************/

/************************ STATIC FUNCTION PROTOTYPES **************************/
#if 0
static void print_bits(
   uint8_t uc_byte);
#endif

/****************************** LOCAL FUNCTIONS *******************************/
CH_IR_RET_E ch_ir_bit_store_init(
   CH_IR_BIT_STORE_CTXT_X **ppx_bit_store_ctxt)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   CH_IR_BIT_STORE_CTXT_X *px_bit_store_ctxt = NULL;

   if (NULL == ppx_bit_store_ctxt)
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   px_bit_store_ctxt = pal_malloc (sizeof(CH_IR_BIT_STORE_CTXT_X), NULL);
   if (NULL == px_bit_store_ctxt)
   {
      CH_IR_LOG_MED ("pal_malloc failed");
      e_ret_val = eCH_IR_RET_RESOURCE_FAILURE;
      goto CLEAN_RETURN;
   }

   px_bit_store_ctxt->ui_cur_allocated_bytes = 1;

   px_bit_store_ctxt->puc_data = pal_malloc (
      px_bit_store_ctxt->ui_cur_allocated_bytes, NULL);
   if (NULL == px_bit_store_ctxt->puc_data)
   {
      CH_IR_LOG_MED("pal_malloc failed");
      e_ret_val = eCH_IR_RET_RESOURCE_FAILURE;
      goto CLEAN_RETURN;
   }

   CH_IR_LOG_MED("Current allocation changed to %d bytes",
      px_bit_store_ctxt->ui_cur_allocated_bytes);

   px_bit_store_ctxt->ui_cur_byte = 0;
   px_bit_store_ctxt->ui_cur_bit = 0;

   *ppx_bit_store_ctxt = px_bit_store_ctxt;
   e_ret_val = eCH_IR_RET_SUCCESS;
CLEAN_RETURN:
   return e_ret_val;
}

CH_IR_RET_E ch_ir_bit_store_deinit(
   CH_IR_BIT_STORE_CTXT_X *px_bit_store_ctxt)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;

   if (NULL == px_bit_store_ctxt)
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   if (NULL != px_bit_store_ctxt->puc_data)
   {
      pal_free (px_bit_store_ctxt->puc_data);
      px_bit_store_ctxt->puc_data = NULL;
   }

   pal_free (px_bit_store_ctxt);
   px_bit_store_ctxt = NULL;
   e_ret_val = eCH_IR_RET_SUCCESS;
CLEAN_RETURN:
   return e_ret_val;
}

#if 0
static void print_bits(
   uint8_t uc_byte)
{
   uint32_t ui_i = 8;
   uint32_t ui_bit = 0;

   while (ui_i > 0)
   {
      ui_bit = uc_byte & 0x1;

      printf ("%d",ui_bit);

      uc_byte = uc_byte >> 1;
      ui_i--;
   }
}
#endif

CH_IR_RET_E ch_ir_bit_store_write_bit(
   CH_IR_BIT_STORE_CTXT_X *px_bit_store_ctxt,
   bool b_bit_value)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   uint8_t uc_cur_byte = 0;
   uint8_t uc_bit_to_write = 0;

   if (NULL == px_bit_store_ctxt)
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   if (px_bit_store_ctxt->ui_cur_byte == px_bit_store_ctxt->ui_cur_allocated_bytes)
   {
      px_bit_store_ctxt->ui_cur_allocated_bytes *= 2;
      CH_IR_LOG_MED("Current allocation changed to %d bytes",
         px_bit_store_ctxt->ui_cur_allocated_bytes);
      px_bit_store_ctxt->puc_data = realloc(px_bit_store_ctxt->puc_data,
         px_bit_store_ctxt->ui_cur_allocated_bytes);

      if (NULL == px_bit_store_ctxt->puc_data)
      {
         CH_IR_LOG_MED("pal_malloc failed");
         e_ret_val = eCH_IR_RET_RESOURCE_FAILURE;
         goto CLEAN_RETURN;
      }
      (void) pal_memset (
         (px_bit_store_ctxt->puc_data + px_bit_store_ctxt->ui_cur_byte), 0x00,
         px_bit_store_ctxt->ui_cur_allocated_bytes / 2);
   }

   uc_cur_byte = px_bit_store_ctxt->puc_data[px_bit_store_ctxt->ui_cur_byte];

   uc_bit_to_write = ((true == b_bit_value) ? (1) : (0));

   uc_cur_byte = uc_cur_byte << 1;

   uc_cur_byte = uc_cur_byte | uc_bit_to_write;

   px_bit_store_ctxt->puc_data[px_bit_store_ctxt->ui_cur_byte] = uc_cur_byte;

   px_bit_store_ctxt->ui_cur_bit++;
   if (8 == px_bit_store_ctxt->ui_cur_bit)
   {
      px_bit_store_ctxt->ui_cur_bit = 0;
      px_bit_store_ctxt->ui_cur_byte++;
   }
   e_ret_val = eCH_IR_RET_SUCCESS;
CLEAN_RETURN:
   return e_ret_val;
}

/*
 * The logic for the following piece of code was referred from the following
 * web link:
 * http://en.wikipedia.org/wiki/Elias_gamma_coding
 */
static void ch_ir_compress_using_gamma_coding (
   CH_IR_BIT_STORE_CTXT_X *px_bit_store_ctxt,
   uint32_t ui_value)
{
   if (ui_value == 1)
   {
      ch_ir_bit_store_write_bit(px_bit_store_ctxt, true);
   }
   else
   {
      ch_ir_bit_store_write_bit(px_bit_store_ctxt, false);
      ch_ir_compress_using_gamma_coding (px_bit_store_ctxt, ui_value / 2);

      ch_ir_bit_store_write_bit(px_bit_store_ctxt,
         ((1 == (ui_value % 2)) ? (true) : (false)));
   }
}

/*
 * The following piece of code was referred from the following web link:
 * http://en.wikipedia.org/wiki/Elias_delta_coding
 */
static void ch_ir_compress_using_delta_coding(
   CH_IR_BIT_STORE_CTXT_X *px_bit_store_ctxt,
   int ui_value)
{
   uint32_t ui_temp = 0;
   uint32_t ui_len = 0;
   uint32_t ui_len_of_len = 0;
   uint32_t ui_i = 0;

   for (ui_temp = ui_value; ui_temp > 0; ui_temp >>= 1)
   {
      // calculate 1+floor(log2(num))
      ui_len++;
   }
   //printf ("Length: %d\n", ui_len);
   for (ui_temp = ui_len; ui_temp > 1; ui_temp >>= 1)
   {
      // calculate floor(log2(len))
      ui_len_of_len++;
   }
   //printf ("Length of length: %d\n", ui_len_of_len);
   for (ui_i = ui_len_of_len; ui_i > 0; --ui_i)
   {
      ch_ir_bit_store_write_bit(px_bit_store_ctxt, false);
   }

   for (ui_i = ui_len_of_len; ui_i > 0; --ui_i)
   {
      ch_ir_bit_store_write_bit(px_bit_store_ctxt,
         ((1 == ((ui_len >> ui_i) & 1)) ? (true) : (false)));
   }
   for (ui_i = ui_len - 1; ui_i > 0; ui_i--)
   {
      ch_ir_bit_store_write_bit(px_bit_store_ctxt,
         ((1 == ((ui_value >> ui_i) & 1)) ? (true) : (false)));
   }
}

static HM_RET_E ch_ir_indexer_postings_hm_for_each_compress_cbk(
   HM_NODE_DATA_X *px_curr_node_data,
   void *p_app_data)
{
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;
   CH_IR_POSTING_X *px_posting = NULL;
   CH_IR_TOKEN_HM_ENTRY_X *px_token_hm_entry = NULL;

   px_token_hm_entry = (CH_IR_TOKEN_HM_ENTRY_X *) p_app_data;
   px_posting = (CH_IR_POSTING_X *) px_curr_node_data->p_data;

#if 0
   printf ("%p, %p\n", px_token_hm_entry->x_compressed_postings.px_doc_id,
      px_token_hm_entry->x_compressed_postings.px_term_freq);
#endif

   ch_ir_compress_using_gamma_coding (
      px_token_hm_entry->x_compressed_postings.px_term_freq,
      px_posting->ui_term_freq);

   ch_ir_compress_using_delta_coding(
      px_token_hm_entry->x_compressed_postings.px_doc_id,
      px_posting->ui_doc_id);

   //printf ("%s (%d,%d,%d), ", px_curr_node_data->u_hm_key.puc_str_key,
   //   px_posting->ui_doc_id, px_posting->ui_term_freq, px_posting->ui_gap);
   e_hm_ret = eHM_RET_SUCCESS;
LBL_CLEANUP:
   return e_hm_ret;
}

static HM_RET_E ch_ir_indexer_token_hm_for_each_compress_cbk(
   HM_NODE_DATA_X *px_curr_node_data,
   void *p_app_data)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;
   CH_IR_TOKEN_HM_ENTRY_X *px_token_hm_entry = NULL;
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt = NULL;
   HM_FOR_EACH_PARAMS_X x_for_each_param = {eHM_FOR_EACH_DIRECTION_INVALID};

   px_indexer_ctxt = (CH_IR_INDEXER_CTXT_X *) p_app_data;
   px_token_hm_entry = (CH_IR_TOKEN_HM_ENTRY_X *) px_curr_node_data->p_data;

   e_ret_val = ch_ir_bit_store_init (
      &(px_token_hm_entry->x_compressed_postings.px_doc_id));
   e_ret_val = ch_ir_bit_store_init (
      &(px_token_hm_entry->x_compressed_postings.px_term_freq));

#if 0
   printf ("%p, %p\n", px_token_hm_entry->x_compressed_postings.px_doc_id,
      px_token_hm_entry->x_compressed_postings.px_term_freq);
#endif

   x_for_each_param.e_data_structure = eHM_DATA_STRUCT_LINKED_LIST;
   x_for_each_param.e_direction = eHM_FOR_EACH_DIRECTION_FORWARD;
   e_hm_ret = hm_for_each_v2 (px_token_hm_entry->x_postings.hl_posting_hm,
      &x_for_each_param, ch_ir_indexer_postings_hm_for_each_compress_cbk,
      px_token_hm_entry);
LBL_CLEANUP:
   return e_hm_ret;
}

CH_IR_RET_E ch_ir_compress_index (
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   HM_FOR_EACH_PARAMS_X x_for_each_param = {eHM_DATA_STRUCT_INVALID};
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;

   x_for_each_param.e_data_structure = eHM_DATA_STRUCT_LINKED_LIST;
   x_for_each_param.e_direction = eHM_FOR_EACH_DIRECTION_FORWARD;
   e_hm_ret = hm_for_each_v2 (px_indexer_ctxt->hl_token_hm, &x_for_each_param,
      ch_ir_indexer_token_hm_for_each_compress_cbk, px_indexer_ctxt);

CLEAN_RETURN:
   return e_ret_val;
}

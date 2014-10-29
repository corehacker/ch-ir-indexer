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
 * \file   ch-ir-common.h
 *
 * \author sandeepprakash
 *
 * \date   Mar 11, 2014
 *
 * \brief  
 *
 ******************************************************************************/

#ifndef __CH_IR_COMMON_H__
#define __CH_IR_COMMON_H__

#ifdef  __cplusplus
extern  "C"
{
#endif

/********************************* CONSTANTS **********************************/

/*********************************** MACROS ***********************************/
#define CH_IR_LOG_STR                         "CHIR"

#define CH_IR_LOG_LOW(format,...)                                              \
do                                                                            \
{                                                                             \
   LOG_LOW (CH_IR_LOG_STR,__FILE__,__FUNCTION__,__LINE__,format,               \
      ##__VA_ARGS__);                                                         \
} while (0)

#define CH_IR_LOG_MED(format,...)                                              \
do                                                                            \
{                                                                             \
   LOG_MED (CH_IR_LOG_STR,__FILE__,__FUNCTION__,__LINE__,format,               \
      ##__VA_ARGS__);                                                         \
} while (0)

#define CH_IR_LOG_HIGH(format,...)                                             \
do                                                                            \
{                                                                             \
   LOG_HIGH (CH_IR_LOG_STR,__FILE__,__FUNCTION__,__LINE__,format,              \
      ##__VA_ARGS__);                                                         \
} while (0)

#define CH_IR_LOG_FULL(format,...)                                             \
do                                                                            \
{                                                                             \
   LOG_FULL (CH_IR_LOG_STR,__FILE__,__FUNCTION__,__LINE__,format,              \
      ##__VA_ARGS__);                                                         \
} while (0)
/******************************** ENUMERATIONS ********************************/
typedef enum _CH_IR_RET_E
{
   eCH_IR_RET_SUCCESS             = 0x00000000,

   eCH_IR_RET_FAILURE,

   eCH_IR_RET_INVALID_ARGS,

   eCH_IR_RET_INVALID_HANDLE,

   eCH_IR_RET_RESOURCE_FAILURE,

   eCH_IR_RET_MAX
} CH_IR_RET_E;
/*********************** CLASS/STRUCTURE/UNION DATA TYPES *********************/

/***************************** FUNCTION PROTOTYPES ****************************/

#ifdef   __cplusplus
}
#endif

#endif /* __CH_IR_COMMON_H__ */

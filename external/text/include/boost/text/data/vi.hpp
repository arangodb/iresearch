// Copyright (C) 2020 T. Zachary Laine
//
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

// Warning! This file is autogenerated.
#ifndef BOOST_TEXT_DATA_VI_HPP
#define BOOST_TEXT_DATA_VI_HPP

#include <boost/text/string_view.hpp>


namespace boost { namespace text { namespace data { namespace vi {

inline string_view standard_collation_tailoring()
{
    return string_view((char const *)
u8R"(  
[normalization on]
&̀<<̉<<̃<<́<<̣
&a<ă<<<Ă<â<<<Â
&d<đ<<<Đ
&e<ê<<<Ê
&o<ô<<<Ô<ơ<<<Ơ
&u<ư<<<Ư
  )");
}

inline string_view traditional_collation_tailoring()
{
    return string_view((char const *)
u8R"(  
[normalization on]
&̀<<̉<<̃<<́<<̣
&a<ă<<<Ă<â<<<Â
&C<ch<<<Ch<<<CH
&d<đ<<<Đ
&e<ê<<<Ê
&G<gh<<<Gh<<<GH<gi<<<Gi<<<GI
&K<kh<<<Kh<<<KH
&N<nh<<<Nh<<<NH<ng<<<Ng<<<NG<ngh<<<Ngh<<<NGh<<<NGH
&o<ô<<<Ô<ơ<<<Ơ
&P<ph<<<Ph<<<PH
&Q<qu<<<Qu<<<QU
&T<th<<<Th<<<TH<tr<<<Tr<<<TR
&u<ư<<<Ư
  )");
}


}}}}

#endif
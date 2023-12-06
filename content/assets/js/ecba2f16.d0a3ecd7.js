"use strict";(self.webpackChunkhudi=self.webpackChunkhudi||[]).push([[28420],{3905:(e,t,r)=>{r.d(t,{Zo:()=>s,kt:()=>g});var n=r(67294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function i(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?i(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):i(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function l(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},i=Object.keys(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var c=n.createContext({}),p=function(e){var t=n.useContext(c),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},s=function(e){var t=p(e.components);return n.createElement(c.Provider,{value:t},e.children)},d="mdxType",u={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,i=e.originalType,c=e.parentName,s=l(e,["components","mdxType","originalType","parentName"]),d=p(r),m=a,g=d["".concat(c,".").concat(m)]||d[m]||u[m]||i;return r?n.createElement(g,o(o({ref:t},s),{},{components:r})):n.createElement(g,o({ref:t},s))}));function g(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=r.length,o=new Array(i);o[0]=m;var l={};for(var c in t)hasOwnProperty.call(t,c)&&(l[c]=t[c]);l.originalType=e,l[d]="string"==typeof e?e:a,o[1]=l;for(var p=2;p<i;p++)o[p]=r[p];return n.createElement.apply(null,o)}return n.createElement.apply(null,r)}m.displayName="MDXCreateElement"},6746:(e,t,r)=>{r.d(t,{Z:()=>i});var n=r(67294),a=r(72389);function i(e){let{children:t,url:i}=e;return(0,a.Z)()&&(r.g.window.location.href=i),n.createElement("span",null,t,"or click ",n.createElement("a",{href:i},"here"))}},35685:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>p,contentTitle:()=>l,default:()=>m,frontMatter:()=>o,metadata:()=>c,toc:()=>s});var n=r(87462),a=(r(67294),r(3905)),i=r(6746);const o={title:"Demystifying Copy-on-Write in Apache Hudi: Understanding Read and Write Operations",excerpt:"COW Overview",author:"Eswaramoorthy P",category:"blog",image:"/assets/images/blog/2023-09-10-Demystifying-Copy-on-Write-in-Apache-Hudi-Understanding-Read-and-Write-Operations.png",tags:["reads","medium","blog","apache hudi","writes","cow"]},l=void 0,c={permalink:"/blog/2023/09/10/Demystifying-Copy-on-Write-in-Apache-Hudi-Understanding-Read-and-Write-Operations",editUrl:"https://github.com/apache/hudi/edit/asf-site/website/blog/blog/2023-09-10-Demystifying-Copy-on-Write-in-Apache-Hudi-Understanding-Read-and-Write-Operations.mdx",source:"@site/blog/2023-09-10-Demystifying-Copy-on-Write-in-Apache-Hudi-Understanding-Read-and-Write-Operations.mdx",title:"Demystifying Copy-on-Write in Apache Hudi: Understanding Read and Write Operations",description:"Redirecting... please wait!!",date:"2023-09-10T00:00:00.000Z",formattedDate:"September 10, 2023",tags:[{label:"reads",permalink:"/blog/tags/reads"},{label:"medium",permalink:"/blog/tags/medium"},{label:"blog",permalink:"/blog/tags/blog"},{label:"apache hudi",permalink:"/blog/tags/apache-hudi"},{label:"writes",permalink:"/blog/tags/writes"},{label:"cow",permalink:"/blog/tags/cow"}],readingTime:.045,truncated:!1,authors:[{name:"Eswaramoorthy P"}],prevItem:{title:"Lakehouse or Warehouse? Part 2 of 2",permalink:"/blog/2023/09/12/Lakehouse-or-Warehouse-Part-2-of-2"},nextItem:{title:"Lakehouse or Warehouse? Part 1 of 2",permalink:"/blog/2023/09/06/Lakehouse-or-Warehouse-Part-1-of-2"}},p={authorsImageUrls:[void 0]},s=[],d={toc:s},u="wrapper";function m(e){let{components:t,...r}=e;return(0,a.kt)(u,(0,n.Z)({},d,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)(i.Z,{url:"https://medium.com/walmartglobaltech/demystifying-copy-on-write-in-apache-hudi-understanding-read-and-write-operations-3aa274017884",mdxType:"Redirect"},"Redirecting... please wait!! "))}m.isMDXComponent=!0}}]);
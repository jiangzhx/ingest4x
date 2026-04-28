# Rules 字段语义

字段路径使用点号语法，例如：

- `xcontext.installid`
- `xcontext.currencyamount`

支持的约束：

- `required: true`
- `type: string | number | integer | boolean | object | array`
- `enum: [...]`
- `gt / gte / lt / lte`
- `required_when`
- `required_any_when`

## 基础示例

```yaml
fields:
  appid:
    required: true
    type: string
  xwhat:
    required: true
    type: string
  xcontext:
    required: true
    type: object
  xcontext.installid:
    required: true
    type: string
  xcontext.os:
    required: true
    type: string
    enum:
      - ios
      - android
      - harmony
      - wechat
      - toutiao
      - tiktok
    required_any_when:
      - equals: ios
        fields: [xcontext.idfa, xcontext.caid]
      - equals: android
        fields: [xcontext.oaid, xcontext.androidid]
```

上面这段表示：

- `appid`、`xwhat`、`xcontext`、`xcontext.installid`、`xcontext.os` 必填
- `xcontext.os` 必须是给定枚举之一
- 当 `xcontext.os=ios` 时，`xcontext.idfa` 和 `xcontext.caid` 至少要有一个
- 当 `xcontext.os=android` 时，`xcontext.oaid` 和 `xcontext.androidid` 至少要有一个

字符串枚举匹配不区分大小写，条件判断里的字符串 `equals` 也不区分大小写。

## 校验失败的典型报错

规则失败时，报错会尽量直接指向字段：

- `missing required field \`xwhat\``
- `field \`xcontext.currencyamount\` expected type \`Number\``
- `field \`xcontext.currencytype\` must be one of [...]`
- `at least one field is required: xcontext.idfa, xcontext.caid`

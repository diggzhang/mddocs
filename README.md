# mddocs

[![Documentation Status](https://readthedocs.org/projects/modoudocs/badge/?version=latest)](https://modoudocs.readthedocs.io/en/latest/?badge=latest)

[魔豆项目官方文档库](https://modoudocs.readthedocs.io/en/latest/)

## 目录

- [背景](#背景)
- [安装](#安装)
- [使用说明](#使用说明)
- [如何贡献](#如何贡献)

## 背景

- [Docs Like Code](https://www.docslikecode.com/about/)
- [Sphinx](https://www.sphinx-doc.org/en/master/tutorial/first-steps.html)
- [ReStructuredText](https://docutils.sourceforge.io/docs/user/rst/quickstart.html)

## 安装

这个项目使用[Sphinx](https://www.sphinx-doc.org/en/master/tutorial/first-steps.html)。请确保你本地安装了它们。

```sh
pip install -U sphinx
# for Linux
yum install python-sphinx
# for macOS
brew install sphinx-doc
```

克隆想项目到本地工作区：

```
git clone git@github.com:diggzhang/mddocs.git && cd mddocs
```

执行`make`列出ming 确保项目环境准备完成。


## 使用说明

### 创建文档

以欢迎页为例，文档使用的是rst标记语言，相对于`markdown`来说，脱离渲染纯文本表达力更强。是sphinx-doc默认支持的文档编写语言。

一般在`source`目录下，创建对应章节的`rst`格式文档：

```shell
# cat source/welcome.rst
welcome
=======

The quick brown fox jumps over the lazy dog.
```

### 编入目录

完成rst文档编写后，需要编辑`source/index.rst`文件，引入目录中。将引入文档的文件名(去后缀)写入`toctree`下：

```
# cat source/index.rst
.. toctree::
   :maxdepth: 2
   :caption: Contents:

   install
   welcome
   ......
```


### 预览

完成编辑后，可以在本地生成预览，在项目目录下执行`make html`:

```shell
# make html
......
The HTML pages are in build/html.
```

完成后可见在项目目录下生成了`build/html`。进入到该目录下用浏览器打开`index.html`预览文档生成结果。

## 如何贡献

1. clone项目后在其它分支编辑文档
2. 审核通过后push到`main`分支，推送该分支会触发readthedocs线上更新[魔豆项目官方文档库](https://modoudocs.readthedocs.io/en/latest/)

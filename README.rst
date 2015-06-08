Sippers
=======


Import and parse CNMC SIPS

.. image:: http://ci.gisce.net/api/badge/github.com/gisce/sippers/status.svg?branch=master
   :target: http://ci.gisce.net/github.com/gisce/sippers
   
Installation
------------

.. code-block:: shell

    $ pip install sippers

Use
---

.. code-block:: shell

    $ sippers import --file /path/to/sips_file.zip \
      --backend mongodb://user:pass@localhost:27017/db

At this moment only **MongoDB** as a backend is supported, but you can write your own backend.
